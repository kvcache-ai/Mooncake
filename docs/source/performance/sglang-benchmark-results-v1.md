## PD Disaggregation Performance

We evaluated the current implementation on two A10 servers. By comparing the performance of a 1P1D configuration with that of two regular (non-disaggregated) instances, we observed that P/D disaggregation achieves approximately 30% lower ITL while maintaining comparable total throughput. This aligns with findings from the Mooncake paper, which highlighted that P/D disaggregation is effective in reducing TBT/ITL under similar throughput conditions—or conversely, in enabling higher throughput under stricter ITL/TBT SLOs.
Moreover, we anticipate even greater benefits in larger-scale clusters where both the number of prefill and decode nodes (x and y in xPyD configurations) increase, offering enhanced scheduling flexibility and resource efficiency.

## Traffic Request Rate: 1.0
* model: Qwen2.5-7B-Instruct-GPTQ-Int4
* TP: 4
* random_input_len=8192, random_output_len=512
* num prompt=50

| Configuration  | Output Token Throughput (tok/s)  | Mean E2E Latency (ms) |Total Token Throughput (tok/s)  | Mean TTFT (ms) | P99 TTFT (ms) | Mean ITL (ms) | P99 ITL (ms) |
|----------------|----------------------------------|-----------------------|----------------|---------------|---------------|--------------|---------------------------------|
| 1P1D           | 407.59                           | 3413.86               |7084.46                         | 732.54         | 2952.57       | 7.23          | 10.76        |
| 2 Regular      | 427.65                           | 4586.54               |7433.27                         | 767.18         | 1264.88       | 10.30         | 12.73        |

## Traffic Request Rate: 4.0
* model: Qwen2.5-7B-Instruct-GPTQ-Int4
* TP: 2
* random_input_len=2048, random_output_len=512
* num prompt=200

| Configuration | Output Token Throughput (tok/s) | Mean E2E Latency (ms)  | Total Token Throughput (tok/s)  | Mean TTFT (ms) |P99 TTFT (ms) | Mean ITL (ms) | P99 ITL (ms) |
|---------------|---------------------------------|-------------|--------------------------------|----------------|---------------|----------------|--------------|
| 1P1D          | 1215.17                         | 11519.24    | 6161.43                        | 1111.94        | 2725.89       | 17.06          | 19.72        |
| 2 Regular     | 1223.03                         | 11683.15    | 6201.29                        | 310.01         | 720.91        | 25.74          | 294.89       |