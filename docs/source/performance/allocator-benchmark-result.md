# Allocator Performance

We evaluated the performance of [OffsetAllocator](https://github.com/sebbbi/OffsetAllocator), the default memory allocator in Mooncake Store. This allocator is responsible for allocating memory from mounted segments to store the KV cache.

In this context, the most important metric is **memory utilization**, defined as the ratio between the amount of memory that can be successfully allocated and the total available memory. A higher utilization means that more KV tensors can be cached, thereby accelerating LLM tasks. However, due to memory fragmentation, allocation may fail even when the allocated memory is well below the total available capacity.

For the same allocator, memory utilization can vary significantly under different workloads. Therefore, we evaluated the allocator's efficiency across a range of workloads.

In particular, in the **LLM inference** scenario, once the model is fixed, the size of each KV vector is also fixed. This means memory utilization under **uniform allocation sizes** becomes especially important. However, the original OffsetAllocator has a limitation: when the allocation size is uniform but does not match any of OffsetAllocator's predefined bin sizes, memory utilization can be suboptimal.

To address this, we introduced targeted optimizations for uniform-size workloads on top of the original OffsetAllocator. As shown in our test results, the optimized version achieves **significant performance improvements** in such scenarios.

## Execution

```bash
./mooncake-store/benchmarks/allocator_bench
```

## Result

- alloc size: The size of each object
- utilization ratio: The total allocated size / total space
- time: time in nanoseconds for each object allocation
- OffsetAllocator optimization: round up the allocated size to a bin size.

### Uniform size, size equals power of 2

**OffsetAllocator (After Optimization)**

| Alloc Size | Min Util Ratio | Avg Util Ratio | Time (ns) |
|------------|----------------|----------------|-----------|
| 32         | 1              | 1              | 544       |
| 128        | 1              | 1              | 417       |
| 512        | 1              | 1              | 174       |
| 2048       | 1              | 1              | 406       |
| 8192       | 1              | 1              | 180       |
| 32768      | 1              | 1              | 133       |
| 131072     | 1              | 1              | 109       |
| 524288     | 1              | 1              | 100       |
| 2097152    | 1              | 1              | 99        |
| 8388608    | 1              | 1              | 99        |
| 33554432   | 1              | 1              | 98        |

**OffsetAllocator (Before Optimization)**

| Alloc Size | Min Util Ratio | Avg Util Ratio | Time (ns) |
|------------|----------------|----------------|-----------|
| 32         | 1              | 1              | 539       |
| 128        | 1              | 1              | 419       |
| 512        | 1              | 1              | 217       |
| 2048       | 1              | 1              | 408       |
| 8192       | 1              | 1              | 175       |
| 32768      | 1              | 1              | 130       |
| 131072     | 1              | 1              | 107       |
| 524288     | 1              | 1              | 99        |
| 2097152    | 1              | 1              | 100       |
| 8388608    | 1              | 1              | 98        |
| 33554432   | 1              | 1              | 98        |

### Uniform size, size equals power of 2 +/- 17

**OffsetAllocator (After Optimization)**

| Alloc Size | Min Util Ratio | Avg Util Ratio | Time (ns) |
|------------|----------------|----------------|-----------|
| 15         | 1              | 1              | 568       |
| 111        | 0.991071       | 0.991071       | 441       |
| 495        | 0.966797       | 0.966797       | 178       |
| 2031       | 0.991699       | 0.991699       | 418       |
| 8175       | 0.997925       | 0.997925       | 170       |
| 32751      | 0.999481       | 0.999481       | 133       |
| 131055     | 0.99987        | 0.99987        | 109       |
| 524271     | 0.999968       | 0.999968       | 100       |
| 2097135    | 0.999992       | 0.999992       | 99        |
| 8388591    | 0.999998       | 0.999998       | 98        |
| 33554415   | 0.999999       | 0.999999       | 99        |
| 49         | 0.942308       | 0.942308       | 508       |
| 145        | 0.906249       | 0.906249       | 372       |
| 529        | 0.918399       | 0.918399       | 172       |
| 2065       | 0.896267       | 0.896267       | 403       |
| 8209       | 0.89073        | 0.89073        | 174       |
| 32785      | 0.889347       | 0.889347       | 131       |
| 131089     | 0.88897        | 0.88897        | 105       |
| 524305     | 0.888701       | 0.888701       | 102       |
| 2097169    | 0.888679       | 0.888679       | 100       |
| 8388625    | 0.886721       | 0.886721       | 100       |
| 33554449   | 0.875          | 0.875          | 100       |

**OffsetAllocator (Before Optimization)**

| Alloc Size | Min Util Ratio | Avg Util Ratio | Time (ns) |
|------------|----------------|----------------|-----------|
| 15         | 1              | 1              | 566       |
| 111        | 0.669866       | 0.710845       | 703       |
| 495        | 0.665779       | 0.676874       | 238       |
| 2031       | 0.668333       | 0.705411       | 637       |
| 8175       | 0.666175       | 0.676474       | 242       |
| 32751      | 0.664435       | 0.669078       | 168       |
| 131055     | 0.66062        | 0.667341       | 124       |
| 524271     | 0.653055       | 0.666993       | 118       |
| 2097135    | 0.64062        | 0.666873       | 116       |
| 8388591    | 0.605468       | 0.667812       | 115       |
| 33554415   | 0.5625         | 0.670944       | 116       |
| 49         | 0.692229       | 0.753062       | 1122      |
| 145        | 0.667789       | 0.700907       | 572       |
| 529        | 0.66577        | 0.676238       | 238       |
| 2065       | 0.667926       | 0.704884       | 632       |
| 8209       | 0.665708       | 0.676372       | 239       |
| 32785      | 0.664224       | 0.669058       | 168       |
| 131089     | 0.659631       | 0.667287       | 129       |
| 524305     | 0.652609       | 0.666884       | 122       |
| 2097169    | 0.638677       | 0.666516       | 120       |
| 8388625    | 0.60547        | 0.665131       | 121       |
| 33554449   | 0.546875       | 0.660917       | 120       |

### Uniform size, size equals power of 2 multiply 0.9 or 1.1

**OffsetAllocator (After Optimization)**

| Alloc Size | Min Util Ratio | Avg Util Ratio | Time (ns) |
|------------|----------------|----------------|-----------|
| 28         | 1              | 1              | 543       |
| 115        | 0.958333       | 0.958333       | 418       |
| 460        | 0.958332       | 0.958332       | 189       |
| 1843       | 0.959896       | 0.959896       | 418       |
| 7372       | 0.959895       | 0.959895       | 197       |
| 29491      | 0.959993       | 0.959993       | 135       |
| 117964     | 0.959979       | 0.959979       | 111       |
| 471859     | 0.959985       | 0.959985       | 100       |
| 1887436    | 0.959765       | 0.959765       | 99        |
| 7549747    | 0.959766       | 0.959766       | 99        |
| 30198988   | 0.95625        | 0.95625        | 99        |
| 35         | 0.972222       | 0.972222       | 531       |
| 140        | 0.972222       | 0.972222       | 397       |
| 563        | 0.977427       | 0.977427       | 180       |
| 2252       | 0.97743        | 0.97743        | 389       |
| 9011       | 0.977752       | 0.977752       | 183       |
| 36044      | 0.977752       | 0.977752       | 133       |
| 144179     | 0.977739       | 0.977739       | 106       |
| 576716     | 0.977538       | 0.977538       | 103       |
| 2306867    | 0.977539       | 0.977539       | 99        |
| 9227468    | 0.975391       | 0.975391       | 99        |
| 36909875   | 0.9625         | 0.9625         | 100       |

**OffsetAllocator (Before Optimization)**

| Alloc Size | Min Util Ratio | Avg Util Ratio | Time (ns) |
|------------|----------------|----------------|-----------|
| 28         | 1              | 1              | 539       |
| 115        | 0.669299       | 0.709245       | 701       |
| 460        | 0.665825       | 0.677532       | 255       |
| 1843       | 0.669352       | 0.709202       | 691       |
| 7372       | 0.66619        | 0.677401       | 260       |
| 29491      | 0.664311       | 0.669511       | 172       |
| 117964     | 0.661812       | 0.667356       | 133       |
| 471859     | 0.654345       | 0.667048       | 123       |
| 1887436    | 0.640722       | 0.666447       | 121       |
| 7549747    | 0.611719       | 0.666847       | 119       |
| 30198988   | 0.548437       | 0.669799       | 125       |
| 35         | 0.7098         | 0.774162       | 1306      |
| 140        | 0.667934       | 0.702151       | 599       |
| 563        | 0.665599       | 0.675548       | 239       |
| 2252       | 0.667371       | 0.701623       | 601       |
| 9011       | 0.665485       | 0.675528       | 244       |
| 36044      | 0.663248       | 0.668912       | 170       |
| 144179     | 0.660308       | 0.666934       | 127       |
| 576716     | 0.654467       | 0.66679        | 122       |
| 2306867    | 0.633789       | 0.666159       | 121       |
| 9227468    | 0.597266       | 0.666037       | 118       |
| 36909875   | 0.55           | 0.669564       | 121       |

### Random Size

**OffsetAllocator (After Optimization)**

```
util ratio (min / p99 / p90 / p50 / max / avg):
0.544250 / 0.713338 / 0.779739 / 0.847867 / 0.952591 / 0.841576
avg alloc time: 145.575738 ns/op
```

**OffsetAllocator (Before Optimization)**

```
util ratio (min / p99 / p90 / p50 / max / avg):
0.569255 / 0.712076 / 0.781224 / 0.855046 / 0.976057 / 0.848873
avg alloc time: 142.508508 ns/op
```