#include <cuda_runtime.h>

#define FEROCE_MAX_BATCH 4096

static unsigned long long *g_dptrs = nullptr;
static unsigned int *g_dlens = nullptr;
static unsigned long long *g_dout = nullptr;

static cudaError_t ensure_scratch() {
    if (g_dptrs) return cudaSuccess;
    cudaError_t err;
    err = cudaMalloc(&g_dptrs, FEROCE_MAX_BATCH * sizeof(unsigned long long));
    if (err != cudaSuccess) return err;
    err = cudaMalloc(&g_dlens, FEROCE_MAX_BATCH * sizeof(unsigned int));
    if (err != cudaSuccess) return err;
    err = cudaMalloc(&g_dout, FEROCE_MAX_BATCH * sizeof(unsigned long long));
    return err;
}

__global__ void sum_batch_kernel(const unsigned char *const *ptrs,
                                 const unsigned int *lens,
                                 unsigned long long *out) {
    int b = blockIdx.x;
    const unsigned char *data = ptrs[b];
    unsigned int len = lens[b];
    unsigned long long local = 0;
    for (unsigned int i = threadIdx.x; i < len; i += blockDim.x) {
        local += data[i];
    }
    atomicAdd(&out[b], local);
}

extern "C" int feroce_sum_batch(const unsigned long long *dptrs,
                                const unsigned int *lens, unsigned int n,
                                unsigned long long *out_host) {
    if (n == 0) return 0;
    if (n > FEROCE_MAX_BATCH) return -1;

    cudaError_t err = ensure_scratch();
    if (err != cudaSuccess) return (int)err;

    err = cudaMemcpy(g_dptrs, dptrs, n * sizeof(unsigned long long), cudaMemcpyHostToDevice);
    if (err != cudaSuccess) return (int)err;
    err = cudaMemcpy(g_dlens, lens, n * sizeof(unsigned int), cudaMemcpyHostToDevice);
    if (err != cudaSuccess) return (int)err;
    err = cudaMemset(g_dout, 0, n * sizeof(unsigned long long));
    if (err != cudaSuccess) return (int)err;

    sum_batch_kernel<<<n, 256>>>((const unsigned char *const *)g_dptrs, g_dlens, g_dout);
    err = cudaGetLastError();
    if (err != cudaSuccess) return (int)err;

    return (int)cudaMemcpy(out_host, g_dout, n * sizeof(unsigned long long),
                           cudaMemcpyDeviceToHost);
}
