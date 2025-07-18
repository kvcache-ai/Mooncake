# Allocator Memory Utilization Benchmark

## Execution

```bash
./mooncake-store/benchmarks/allocator_bench
```

## Result

- alloc size: The size of each object
- utilization ratio: The total allocated size / total space
- time: time in nanoseconds for each object allocation
- OffsetAllocator optimization: whether round up the allocated size to a bin size

### Uniform size, size equals power of 2

**OffsetAllocator (After Optimization)**

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

**OffsetAllocator (Before Optimization)**

```
Alloc size: 32, min util ratio: 1, avg util ratio: 1, time: 539 ns
Alloc size: 128, min util ratio: 1, avg util ratio: 1, time: 419 ns
Alloc size: 512, min util ratio: 1, avg util ratio: 1, time: 217 ns
Alloc size: 2048, min util ratio: 1, avg util ratio: 1, time: 408 ns
Alloc size: 8192, min util ratio: 1, avg util ratio: 1, time: 175 ns
Alloc size: 32768, min util ratio: 1, avg util ratio: 1, time: 130 ns
Alloc size: 131072, min util ratio: 1, avg util ratio: 1, time: 107 ns
Alloc size: 524288, min util ratio: 1, avg util ratio: 1, time: 99 ns
Alloc size: 2097152, min util ratio: 1, avg util ratio: 1, time: 100 ns
Alloc size: 8388608, min util ratio: 1, avg util ratio: 1, time: 98 ns
Alloc size: 33554432, min util ratio: 1, avg util ratio: 1, time: 98 ns
```

### Uniform size, size equals power of 2 +/- 17

**OffsetAllocator (After Optimization)**

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

**OffsetAllocator (Before Optimization)**

```
Alloc size: 15, min util ratio: 1, avg util ratio: 1, time: 566 ns
Alloc size: 111, min util ratio: 0.669866, avg util ratio: 0.710845, time: 703 ns
Alloc size: 495, min util ratio: 0.665779, avg util ratio: 0.676874, time: 238 ns
Alloc size: 2031, min util ratio: 0.668333, avg util ratio: 0.705411, time: 637 ns
Alloc size: 8175, min util ratio: 0.666175, avg util ratio: 0.676474, time: 242 ns
Alloc size: 32751, min util ratio: 0.664435, avg util ratio: 0.669078, time: 168 ns
Alloc size: 131055, min util ratio: 0.66062, avg util ratio: 0.667341, time: 124 ns
Alloc size: 524271, min util ratio: 0.653055, avg util ratio: 0.666993, time: 118 ns
Alloc size: 2097135, min util ratio: 0.64062, avg util ratio: 0.666873, time: 116 ns
Alloc size: 8388591, min util ratio: 0.605468, avg util ratio: 0.667812, time: 115 ns
Alloc size: 33554415, min util ratio: 0.5625, avg util ratio: 0.670944, time: 116 ns
Alloc size: 49, min util ratio: 0.692229, avg util ratio: 0.753062, time: 1122 ns
Alloc size: 145, min util ratio: 0.667789, avg util ratio: 0.700907, time: 572 ns
Alloc size: 529, min util ratio: 0.66577, avg util ratio: 0.676238, time: 238 ns
Alloc size: 2065, min util ratio: 0.667926, avg util ratio: 0.704884, time: 632 ns
Alloc size: 8209, min util ratio: 0.665708, avg util ratio: 0.676372, time: 239 ns
Alloc size: 32785, min util ratio: 0.664224, avg util ratio: 0.669058, time: 168 ns
Alloc size: 131089, min util ratio: 0.659631, avg util ratio: 0.667287, time: 129 ns
Alloc size: 524305, min util ratio: 0.652609, avg util ratio: 0.666884, time: 122 ns
Alloc size: 2097169, min util ratio: 0.638677, avg util ratio: 0.666516, time: 120 ns
Alloc size: 8388625, min util ratio: 0.60547, avg util ratio: 0.665131, time: 121 ns
Alloc size: 33554449, min util ratio: 0.546875, avg util ratio: 0.660917, time: 120 ns
```

### Uniform size, size equals power of 2 multiply 0.9 or 1.1

**OffsetAllocator (After Optimization)**

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

**OffsetAllocator (Before Optimization)**

```
Alloc size: 28, min util ratio: 1, avg util ratio: 1, time: 539 ns
Alloc size: 115, min util ratio: 0.669299, avg util ratio: 0.709245, time: 701 ns
Alloc size: 460, min util ratio: 0.665825, avg util ratio: 0.677532, time: 255 ns
Alloc size: 1843, min util ratio: 0.669352, avg util ratio: 0.709202, time: 691 ns
Alloc size: 7372, min util ratio: 0.66619, avg util ratio: 0.677401, time: 260 ns
Alloc size: 29491, min util ratio: 0.664311, avg util ratio: 0.669511, time: 172 ns
Alloc size: 117964, min util ratio: 0.661812, avg util ratio: 0.667356, time: 133 ns
Alloc size: 471859, min util ratio: 0.654345, avg util ratio: 0.667048, time: 123 ns
Alloc size: 1887436, min util ratio: 0.640722, avg util ratio: 0.666447, time: 121 ns
Alloc size: 7549747, min util ratio: 0.611719, avg util ratio: 0.666847, time: 119 ns
Alloc size: 30198988, min util ratio: 0.548437, avg util ratio: 0.669799, time: 125 ns
Alloc size: 35, min util ratio: 0.7098, avg util ratio: 0.774162, time: 1306 ns
Alloc size: 140, min util ratio: 0.667934, avg util ratio: 0.702151, time: 599 ns
Alloc size: 563, min util ratio: 0.665599, avg util ratio: 0.675548, time: 239 ns
Alloc size: 2252, min util ratio: 0.667371, avg util ratio: 0.701623, time: 601 ns
Alloc size: 9011, min util ratio: 0.665485, avg util ratio: 0.675528, time: 244 ns
Alloc size: 36044, min util ratio: 0.663248, avg util ratio: 0.668912, time: 170 ns
Alloc size: 144179, min util ratio: 0.660308, avg util ratio: 0.666934, time: 127 ns
Alloc size: 576716, min util ratio: 0.654467, avg util ratio: 0.66679, time: 122 ns
Alloc size: 2306867, min util ratio: 0.633789, avg util ratio: 0.666159, time: 121 ns
Alloc size: 9227468, min util ratio: 0.597266, avg util ratio: 0.666037, time: 118 ns
Alloc size: 36909875, min util ratio: 0.55, avg util ratio: 0.669564, time: 121 ns
```

### Random Size

**OffsetAllocator (After Optimization)**

```
util ratio (min / p99 / p90 / p50 / max / avg): 0.544250 / 0.713338 / 0.779739 / 0.847867 / 0.952591 / 0.841576
avg alloc time: 145.575738 ns/op
```

**OffsetAllocator (Before Optimization)**

```
util ratio (min / p99 / p90 / p50 / max / avg): 0.569255 / 0.712076 / 0.781224 / 0.855046 / 0.976057 / 0.848873
avg alloc time: 142.508508 ns/op
```