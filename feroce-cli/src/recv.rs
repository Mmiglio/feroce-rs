use feroce::rdma;
use feroce::rdma::buffer_pool::BufferAllocator;
use feroce::{BufferPool, CompletionChannel, ConnectedRdmaEndpoint, QueuePair};
use log::{debug, error, info, warn};
use std::path::Path;
use std::sync::Arc;

use feroce_runtime::{Metric, StreamMetrics};

use crate::dump::{DumpSink, DumpSinkFactory};
use crate::{CmOpts, LogBuffer, RdmaOpts, common::run_session};

pub fn run<A, S>(
    cm_opts: &CmOpts,
    rdma_opts: &RdmaOpts,
    allocator: A,
    dump_factory: Option<S>,
    check_imm: bool,
    stats_out: Option<&Path>,
    log_buffer: LogBuffer,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: BufferAllocator,
    S: DumpSinkFactory,
{
    // spawn poller closure
    let spawn_poller =
        |rdma_endpoint: ConnectedRdmaEndpoint<A>, stream_id: u32, metrics: StreamMetrics| {
            let dump = dump_factory.as_ref().map(|f| {
                f.make(stream_id)
                    .expect("failed to open dump sink for stream")
            });

            // in the thread this would race the first tick, which fixes the CSV columns
            let imm = check_imm.then(|| ImmCounters {
                gaps: metrics.group.register("imm_gaps"),
                missing: metrics.group.register("no_imm"),
            });

            std::thread::spawn({
                let qp = Arc::clone(&rdma_endpoint.qp);
                move || {
                    if let Err(e) = poller_thread(
                        qp,
                        rdma_endpoint.buffer_pool,
                        rdma_endpoint.comp_channel,
                        stream_id,
                        metrics,
                        dump,
                        imm,
                    ) {
                        error!("poller thread error: {}", e);
                    }
                }
            })
        };

    run_session(
        cm_opts,
        rdma_opts,
        allocator,
        spawn_poller,
        stats_out,
        log_buffer,
    )
}

struct ImmCounters {
    gaps: Arc<Metric>,
    missing: Arc<Metric>,
}

fn poller_thread<A: BufferAllocator, D: DumpSink>(
    qp: Arc<QueuePair>,
    buffer_pool: BufferPool<A>,
    channel: Arc<CompletionChannel>,
    stream_id: u32,
    metrics: StreamMetrics,
    mut dump: Option<D>,
    imm: Option<ImmCounters>,
) -> Result<(), Box<dyn std::error::Error>> {
    // metrics
    let mut total_bytes;
    let mut total_msgs;

    // immediate sequence check
    let mut expected_imm = None;

    // start by pre-building and pre-posting recv request
    let mut sge_list = Vec::<Vec<rdma::ibv_sge>>::new();
    let mut recv_wr_list = Vec::<rdma::ibv_recv_wr>::new();

    // build wr_list and prepiost all buffers
    for idx in 0..buffer_pool.num_buf() {
        let buf_handle = buffer_pool.get_handle(idx);
        // 1 sge for now
        sge_list.push(vec![rdma::ibv_sge {
            addr: buf_handle.addr as u64,
            length: buf_handle.len as u32,
            lkey: buf_handle.lkey,
        }]);

        recv_wr_list.push(QueuePair::build_recv_wr(idx as u64, &mut sge_list[idx]));

        qp.post_recv(&mut recv_wr_list[idx])?;
    }

    // create all work completions
    let mut wc_list = vec![rdma::ibv_wc::default(); buffer_pool.num_buf()];

    // request notification from completion channel for every event
    qp.recv_cq().req_notify_cq(false)?;

    let mut poller_done = false;
    while !poller_done {
        // wait for completion event, blocking with timeout of 10 ms
        let cq_event = channel.try_get_cq_event(10)?;

        // rearm the notification
        if cq_event.is_some() {
            qp.recv_cq().req_notify_cq(false)?;
        }

        // poll the CQ (we do it regardless the presence of an event, to avoid race conditions)
        let num_wce = qp.recv_cq().poll(&mut wc_list)?;

        // finally, process completions
        total_bytes = 0;
        total_msgs = 0;
        for (ce_idx, wce) in wc_list.iter().enumerate().take(num_wce) {
            if wce.status != rdma::ibv_wc_status::IBV_WC_SUCCESS {
                if wce.status == rdma::ibv_wc_status::IBV_WC_WR_FLUSH_ERR {
                    debug!("Got IBV_WC_WR_FLUSH_ERR, QP is most likely being closed");
                } else {
                    error!("WC index {} error status: {}", ce_idx, wce.status as i32)
                }
                poller_done = true;
                break;
            }

            // update metrics
            total_bytes += wce.byte_len as u64;
            total_msgs += 1;

            if let Some(counters) = &imm {
                match wce.imm() {
                    Some(imm) => {
                        let exp = expected_imm;
                        if let Some(delta) = check_imm_seq(&mut expected_imm, imm) {
                            warn!(
                                "stream {}: imm gap, expected {}, got {}, delta {}",
                                stream_id,
                                exp.unwrap(),
                                imm,
                                delta
                            );
                            counters.gaps.inc();
                        }
                    }
                    None => counters.missing.inc(),
                }
            }

            // optional payload dump
            if let Some(dump) = dump.as_mut() {
                let buf_handle = buffer_pool.get_handle(wce.wr_id as usize);
                dump.record(buf_handle.addr, wce.byte_len, wce.imm().unwrap_or(0))?;
            }

            // immediately repost buffer
            qp.post_recv(&mut recv_wr_list[wce.wr_id as usize])?;
        }

        metrics.messages.add(total_msgs);
        metrics.bytes.add(total_bytes);
    }

    if let Some(imm) = &imm {
        info!(
            "stream {}: {} imm gaps, {} messages without imm",
            stream_id,
            imm.gaps.value(),
            imm.missing.value()
        );
    }

    Ok(())
}

fn check_imm_seq(expected: &mut Option<u32>, imm: u32) -> Option<i32> {
    let delta = match *expected {
        Some(exp) if imm != exp => Some(imm.wrapping_sub(exp) as i32),
        _ => None,
    };
    *expected = Some(imm.wrapping_add(1));
    delta
}
