# Allocator Memory Utilization Benchmark

## Execution

```bash
./mooncake-store/benchmarks/allocator_bench
```

## Result

- alloc size: The size of each object
- utilization ratio: The total allocated size / total space
- time: time in nanoseconds for each object allocation

**Uniform size, size equals power of 2**

```
Alloc size: 32, min util ratio: 1, avg util ratio: 1, time: 544 ns
Alloc size: 128, min util ratio: 1, avg util ratio: 1, time: 417 ns
Alloc size: 512, min util ratio: 1, avg util ratio: 1, time: 174 ns
Alloc size: 2048, min util ratio: 1, avg util ratio: 1, time: 406 ns
Alloc size: 8192, min util ratio: 1, avg util ratio: 1, time: 180 ns
Alloc size: 32768, min util ratio: 1, avg util ratio: 1, time: 133 ns
Alloc size: 131072, min util ratio: 1, avg util ratio: 1, time: 109 ns
Alloc size: 524288, min util ratio: 1, avg util ratio: 1, time: 100 ns
Alloc size: 2097152, min util ratio: 1, avg util ratio: 1, time: 99 ns
Alloc size: 8388608, min util ratio: 1, avg util ratio: 1, time: 99 ns
Alloc size: 33554432, min util ratio: 1, avg util ratio: 1, time: 98 ns
```
**Uniform size, size equals power of 2 +/- 17**

```
Alloc size: 15, min util ratio: 1, avg util ratio: 1, time: 568 ns
Alloc size: 111, min util ratio: 0.991071, avg util ratio: 0.991071, time: 441 ns
Alloc size: 495, min util ratio: 0.966797, avg util ratio: 0.966797, time: 178 ns
Alloc size: 2031, min util ratio: 0.991699, avg util ratio: 0.991699, time: 418 ns
Alloc size: 8175, min util ratio: 0.997925, avg util ratio: 0.997925, time: 170 ns
Alloc size: 32751, min util ratio: 0.999481, avg util ratio: 0.999481, time: 133 ns
Alloc size: 131055, min util ratio: 0.99987, avg util ratio: 0.99987, time: 109 ns
Alloc size: 524271, min util ratio: 0.999968, avg util ratio: 0.999968, time: 100 ns
Alloc size: 2097135, min util ratio: 0.999992, avg util ratio: 0.999992, time: 99 ns
Alloc size: 8388591, min util ratio: 0.999998, avg util ratio: 0.999998, time: 98 ns
Alloc size: 33554415, min util ratio: 0.999999, avg util ratio: 0.999999, time: 99 ns
Alloc size: 49, min util ratio: 0.942308, avg util ratio: 0.942308, time: 508 ns
Alloc size: 145, min util ratio: 0.906249, avg util ratio: 0.906249, time: 372 ns
Alloc size: 529, min util ratio: 0.918399, avg util ratio: 0.918399, time: 172 ns
Alloc size: 2065, min util ratio: 0.896267, avg util ratio: 0.896267, time: 403 ns
Alloc size: 8209, min util ratio: 0.89073, avg util ratio: 0.89073, time: 174 ns
Alloc size: 32785, min util ratio: 0.889347, avg util ratio: 0.889347, time: 131 ns
Alloc size: 131089, min util ratio: 0.88897, avg util ratio: 0.88897, time: 105 ns
Alloc size: 524305, min util ratio: 0.888701, avg util ratio: 0.888701, time: 102 ns
Alloc size: 2097169, min util ratio: 0.888679, avg util ratio: 0.888679, time: 100 ns
Alloc size: 8388625, min util ratio: 0.886721, avg util ratio: 0.886721, time: 100 ns
Alloc size: 33554449, min util ratio: 0.875, avg util ratio: 0.875, time: 100 ns
```

**Uniform size, size equals power of 2 multiply 0.9 or 1.1**

```
Alloc size: 28, min util ratio: 1, avg util ratio: 1, time: 543 ns
Alloc size: 115, min util ratio: 0.958333, avg util ratio: 0.958333, time: 418 ns
Alloc size: 460, min util ratio: 0.958332, avg util ratio: 0.958332, time: 189 ns
Alloc size: 1843, min util ratio: 0.959896, avg util ratio: 0.959896, time: 418 ns
Alloc size: 7372, min util ratio: 0.959895, avg util ratio: 0.959895, time: 197 ns
Alloc size: 29491, min util ratio: 0.959993, avg util ratio: 0.959993, time: 135 ns
Alloc size: 117964, min util ratio: 0.959979, avg util ratio: 0.959979, time: 111 ns
Alloc size: 471859, min util ratio: 0.959985, avg util ratio: 0.959985, time: 100 ns
Alloc size: 1887436, min util ratio: 0.959765, avg util ratio: 0.959765, time: 99 ns
Alloc size: 7549747, min util ratio: 0.959766, avg util ratio: 0.959766, time: 99 ns
Alloc size: 30198988, min util ratio: 0.95625, avg util ratio: 0.95625, time: 99 ns
Alloc size: 35, min util ratio: 0.972222, avg util ratio: 0.972222, time: 531 ns
Alloc size: 140, min util ratio: 0.972222, avg util ratio: 0.972222, time: 397 ns
Alloc size: 563, min util ratio: 0.977427, avg util ratio: 0.977427, time: 180 ns
Alloc size: 2252, min util ratio: 0.97743, avg util ratio: 0.97743, time: 389 ns
Alloc size: 9011, min util ratio: 0.977752, avg util ratio: 0.977752, time: 183 ns
Alloc size: 36044, min util ratio: 0.977752, avg util ratio: 0.977752, time: 133 ns
Alloc size: 144179, min util ratio: 0.977739, avg util ratio: 0.977739, time: 106 ns
Alloc size: 576716, min util ratio: 0.977538, avg util ratio: 0.977538, time: 103 ns
Alloc size: 2306867, min util ratio: 0.977539, avg util ratio: 0.977539, time: 99 ns
Alloc size: 9227468, min util ratio: 0.975391, avg util ratio: 0.975391, time: 99 ns
Alloc size: 36909875, min util ratio: 0.9625, avg util ratio: 0.9625, time: 100 ns
```

**Random Size**

```
util ratio (min / p99 / p90 / p50 / max / avg): 0.544250 / 0.713338 / 0.779739 / 0.847867 / 0.952591 / 0.841576
avg alloc time: 145.575738 ns/op
```