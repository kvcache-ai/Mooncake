# Benchmark performance on NVIDIA A10
We have implemented vLLM integration, which uses Transfer Engine as the network layer instead of `nccl` and `gloo`, to support **inter-node KVCache transfer**. Transfer Engine provides simpler interface and more efficient use of RDMA devices. Here are some benchmark results.
## Large prefill length with low request rate: input_len=1024, qps = 2

| Backend/Setting                                              | Successful Requests | Duration (s) | Total Input Tokens | Total Generated Tokens | Req Throughput (req/s) | Output Token Throughput (tok/s) | Total Token Throughput (tok/s) | Mean TTFT (ms) | Median TTFT (ms) | P99 TTFT (ms) | Mean TPOT (ms) | Median TPOT (ms) | P99 TPOT (ms) | Mean ITL (ms) | Median ITL (ms) | P99 ITL (ms) |
|--------------------------------------------------------------|---------------------|--------------|--------------------|------------------------|------------------------|---------------------------------|-------------------------------|----------------|-----------------|--------------|---------------|------------------|--------------|--------------|----------------|-------------|
| Non-disaggregated                                            | 200                 | 99.24        | 202314             | 1200                   | 2.02                   | 12.09                           | 2050.81                      | 461.52         | 282.00           | 1734.22      | 99.97         | 17.10            | 671.70       | 99.88         | 12.87           | 2059.38     |
| Non-disaggregated with `--enable-chunked-prefill`            | 200                 | 99.19        | 202314             | 1200                   | 2.02                   | 12.10                           | 2051.66                      | 429.37         | 286.09           | 1166.17      | 56.57         | 34.80            | 135.60       | 56.55         | 12.61           | 157.11      |
| MooncakeTransferEngine with RDMA backend      | 200                 | 99.46        | 202314             | 1200                   | 2.01                   | 12.07                           | 2046.28                      | 1272.55        | 743.54           | 5156.62      | 52.74         | 12.46            | 524.23       | 52.60         | 12.05           | 1136.04     |
| MooncakeTransferEngine with 2 RDMA NIC devices | 200              | 99.43        | 202314             | 1200                   | 2.01                   | 12.07                           | 2046.78                      | 1165.25        | 678.74           | 4576.57      | 48.90         | 12.42            | 444.18       | 48.76         | 11.98           | 606.88      |
| MooncakeTransferEngine with TCP backend       | 200                 | 99.49        | 202314             | 1200                   | 2.01                   | 12.06                           | 2045.51                      | 1925.52        | 1011.58          | 8149.52      | 100.98        | 13.64            | 957.56       | 100.73        | 12.42           | 2536.78     |

 - Currently, non-disaggregated use-cases have lower TTFT (Time to First Token) compared to the disaggregated demos because the non-disaggregated use-cases do not need to perform inter-node communication.
 - By supporting Topology Aware Path Selection and multi-card bandwidth aggregation, the MooncakeTransferEngine with RDMA backend outperforms the TCP backend, demonstrating the advantages of RDMA in terms of low latency and high bandwidth. Especially, the use of 2 RDMA NIC devices for bandwidth aggregation further reduces TTFT, showing that bandwidth aggregation is beneficial for large prefill length transfers. In the future, we will further improve TTFT through GPUDirect RDMA and zero-copy.
 - Since prefill and decoding phases are separated in the disaggregated demos, the workload on the decoding instance becomes more stable, resulting in lower latency in TPOT, as the decode instances can focus on the decoding iterations and will not be disrupted by prefill or chunked prefill requests. However, the system utilization of the prefill instance might be a new problem if the prefill workload is lightweight.

### Short prefill length with varying request rates: input_len=256, qps=2, 4, 6, 8
| QPS | Backend/Setting                                               | Successful Requests | Duration (s) | Total Input Tokens | Total Generated Tokens | Req Throughput (req/s) | Output Token Throughput (tok/s) | Total Token Throughput (tok/s) | Mean TTFT (ms) | Median TTFT (ms) | P99 TTFT (ms) | Mean TPOT (ms) | Median TPOT (ms) | P99 TPOT (ms) | Mean ITL (ms) | Median ITL (ms) | P99 ITL (ms) |
|-----|---------------------------------------------------------------|---------------------|--------------|--------------------|------------------------|------------------------|---------------------------------|-------------------------------|----------------|-----------------|--------------|---------------|------------------|--------------|--------------|----------------|-------------|
| 2   | Non-disaggregated                                             | 200                 | 98.72        | 49054              | 1200                   | 2.03                   | 12.16                           | 509.06                        | 63.68          | 55.86           | 134.96       | 14.87         | 12.19            | 39.43        | 14.85         | 12.17           | 59.28       |
| 2   | Non-disaggregated with `--enable-chunked-prefill`             | 200                 | 98.72        | 49054              | 1200                   | 2.03                   | 12.16                           | 509.05                        | 64.55          | 56.19           | 136.14       | 14.29         | 12.20            | 34.41        | 14.27         | 12.21           | 45.76       |
| 2   | MooncakeTransferEngine with RDMA backend       | 200                 | 98.70        | 49054              | 1200                   | 2.03                   | 12.16                           | 509.14                        | 103.54         | 89.44           | 223.15       | 12.02         | 11.49            | 16.28        | 11.98         | 11.46           | 30.30       |
| 2   | MooncakeTransferEngine with 2 RDMA NIC devices | 200              | 98.71        | 49054              | 1200                   | 2.03                   | 12.16                           | 509.11                        | 101.80         | 88.78           | 220.65       | 11.98         | 11.46            | 16.68        | 11.95         | 11.44           | 30.70       |
| 2   | MooncakeTransferEngine with TCP backend        | 200                 | 98.71        | 49054              | 1200                   | 2.03                   | 12.16                           | 509.09                        | 111.86         | 95.78           | 241.02       | 12.14         | 11.45            | 17.66        | 12.11         | 11.44           | 36.33       |
| 4   | Non-disaggregated                                             | 200                 | 49.46        | 49054              | 1200                   | 4.04                   | 24.26                           | 1016.08                       | 74.93          | 64.54           | 197.32       | 19.09         | 12.27            | 84.70        | 19.04         | 12.25           | 121.41      |
| 4   | Non-disaggregated with `--enable-chunked-prefill`             | 200                 | 49.45        | 49054              | 1200                   | 4.04                   | 24.27                           | 1016.31                       | 76.65          | 65.66           | 185.25       | 17.46         | 12.25            | 58.56        | 17.44         | 12.27           | 69.81       |
| 4   | MooncakeTransferEngine with RDMA backend       | 200                 | 49.48        | 49054              | 1200                   | 4.04                   | 24.25                           | 1015.58                       | 122.52         | 91.97           | 300.48       | 13.30         | 11.52            | 40.96        | 13.27         | 11.48           | 54.24       |
| 4   | MooncakeTransferEngine with 2 RDMA NIC devices | 200              | 49.50        | 49054              | 1200                   | 4.04                   | 24.24                           | 1015.19                       | 122.88         | 90.74           | 303.25       | 13.33         | 11.52            | 42.81        | 13.30         | 11.48           | 55.40       |
| 4   | MooncakeTransferEngine with TCP backend        | 200                 | 49.50        | 49054              | 1200                   | 4.04                   | 24.24                           | 1015.29                       | 132.38         | 99.81           | 329.00       | 14.50         | 11.51            | 77.32        | 14.47         | 11.49           | 77.08       |
| 6   | Non-disaggregated                                             | 200                 | 33.10        | 49054              | 1200                   | 6.04                   | 36.26                           | 1518.46                       | 91.94          | 74.35           | 269.85       | 27.47         | 16.11            | 121.31       | 27.40         | 12.35           | 242.38      |
| 6   | Non-disaggregated with `--enable-chunked-prefill`             | 200                 | 33.10        | 49054              | 1200                   | 6.04                   | 36.25                           | 1518.19                       | 92.19          | 76.48           | 253.62       | 22.06         | 16.43            | 77.50        | 22.04         | 12.34           | 127.60      |
| 6   | MooncakeTransferEngine with RDMA backend       | 200                 | 33.15        | 49054              | 1200                   | 6.03                   | 36.20                           | 1515.84                       | 155.67         | 118.36          | 629.47       | 16.28         | 11.62            | 57.13        | 16.23         | 11.53           | 116.64      |
| 6   | MooncakeTransferEngine with 2 RDMA NIC devices | 200              | 33.15        | 49054              | 1200                   | 6.03                   | 36.20                           | 1515.83                       | 151.39         | 118.74          | 469.07       | 15.97         | 11.61            | 72.20        | 15.93         | 11.53           | 116.50      |
| 6   | MooncakeTransferEngine with TCP backend        | 200                 | 33.17        | 49054              | 1200                   | 6.03                   | 36.18                           | 1515.03                       | 173.83         | 134.61          | 692.35       | 19.74         | 11.62            | 99.98        | 19.69         | 11.53           | 140.50      |
| 8   | Non-disaggregated                                             | 200                 | 24.94        | 49054              | 1200                   | 8.02                   | 48.12                           | 2015.39                       | 120.43         | 88.21           | 397.35       | 42.95         | 24.63            | 182.10       | 42.87         | 12.55           | 561.46      |
| 8   | Non-disaggregated with `--enable-chunked-prefill`             | 200                 | 24.93        | 49054              | 1200                   | 8.02                   | 48.13                           | 2015.49                       | 120.73         | 91.26           | 340.52       | 33.93         | 22.25            | 131.26       | 33.91         | 12.56           | 151.47      |
| 8   | MooncakeTransferEngine with RDMA backend       | 200                 | 25.00        | 49054              | 1200                   | 8.00                   | 48.01                           | 2010.42                       | 238.67         | 154.85          | 937.64       | 24.58         | 12.26            | 172.75       | 24.51         | 11.61           | 301.37      |
| 8   | MooncakeTransferEngine with 2 RDMA NIC devices | 200              | 24.99        | 49054              | 1200                   | 8.00                   | 48.02                           | 2011.02                       | 241.09         | 155.78          | 959.04       | 23.43         | 12.27            | 196.75       | 23.35         | 11.62           | 323.14      |
| 8   | MooncakeTransferEngine with TCP backend        | 200                 | 25.02        | 49054              | 1200                   | 8.00                   | 47.97                           | 2008.92                       | 282.11         | 170.38          | 1168.48      | 25.57         | 12.43            | 211.11       | 25.48         | 11.69           | 415.90      |

- The MooncakeTransferEngine with RDMA backend consistently outperforms the TCP backend across all request rates, highlighting the continuous advantage of RDMA in low latency and high bandwidth transfers.

- As the request rate increases, both TTFT and TPOT (Time per Output Token) increase, reflecting longer response times under higher load. However, bandwidth aggregation continues to show advantages, particularly in TPOT, under higher request rates.

# Raw benchmark results
## Large prefill length with low request rate: input_len=1024, qps = 2
### Non-disaggregated
```
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 1024 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-2.json --request-rate 2
WARNING 11-07 11:27:36 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=2.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-2.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=1024, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 2.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [01:39<00:00,  2.02it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  99.24     
Total input tokens:                      202314    
Total generated tokens:                  1200      
Request throughput (req/s):              2.02      
Output token throughput (tok/s):         12.09     
Total Token throughput (tok/s):          2050.81   
---------------Time to First Token----------------
Mean TTFT (ms):                          461.52    
Median TTFT (ms):                        282.00    
P99 TTFT (ms):                           1734.22   
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          99.97     
Median TPOT (ms):                        17.10     
P99 TPOT (ms):                           671.70    
---------------Inter-token Latency----------------
Mean ITL (ms):                           99.88     
Median ITL (ms):                         12.87     
P99 ITL (ms):                            2059.38   
==================================================
```

### Non-disaggregated with `--enable-chunked-prefill` enabled
```
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 1024 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-2.json --request-rate 2
WARNING 11-06 15:06:13 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=2.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-2.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=1024, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 2.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [01:39<00:00,  2.02it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  99.19     
Total input tokens:                      202314    
Total generated tokens:                  1200      
Request throughput (req/s):              2.02      
Output token throughput (tok/s):         12.10     
Total Token throughput (tok/s):          2051.66   
---------------Time to First Token----------------
Mean TTFT (ms):                          429.37    
Median TTFT (ms):                        286.09    
P99 TTFT (ms):                           1166.17   
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          56.57     
Median TPOT (ms):                        34.80     
P99 TPOT (ms):                           135.60    
---------------Inter-token Latency----------------
Mean ITL (ms):                           56.55     
Median ITL (ms):                         12.61     
P99 ITL (ms):                            157.11    
==================================================
```

### MooncakeTransferEngine with RDMA backend
```
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 1024 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-2.json --request-rate 2
WARNING 11-06 14:46:29 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=2.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-2.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=1024, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 2.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [01:39<00:00,  2.01it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  99.46     
Total input tokens:                      202314    
Total generated tokens:                  1200      
Request throughput (req/s):              2.01      
Output token throughput (tok/s):         12.07     
Total Token throughput (tok/s):          2046.28   
---------------Time to First Token----------------
Mean TTFT (ms):                          1272.55   
Median TTFT (ms):                        743.54    
P99 TTFT (ms):                           5156.62   
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          52.74     
Median TPOT (ms):                        12.46     
P99 TPOT (ms):                           524.23    
---------------Inter-token Latency----------------
Mean ITL (ms):                           52.60     
Median ITL (ms):                         12.05     
P99 ITL (ms):                            1136.04   
==================================================
```

### MooncakeTransferEngine with multiple RDMA NIC devices
```
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 1024 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-2.json --request-rate 2
WARNING 11-11 16:18:59 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=2.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-2.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=1024, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 2.0
100%|███████████████████████████████████████████████████████████████████████| 200/200 [01:39<00:00,  2.01it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  99.43     
Total input tokens:                      202314    
Total generated tokens:                  1200      
Request throughput (req/s):              2.01      
Output token throughput (tok/s):         12.07     
Total Token throughput (tok/s):          2046.78   
---------------Time to First Token----------------
Mean TTFT (ms):                          1165.25   
Median TTFT (ms):                        678.74    
P99 TTFT (ms):                           4576.57   
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          48.90     
Median TPOT (ms):                        12.42     
P99 TPOT (ms):                           444.18    
---------------Inter-token Latency----------------
Mean ITL (ms):                           48.76     
Median ITL (ms):                         11.98     
P99 ITL (ms):                            606.88    
==================================================
```

### MooncakeTransferEngine with TCP backend
```
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 1024 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-2.json --request-rate 2
WARNING 11-06 14:52:00 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=2.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-2.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=1024, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 2.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [01:39<00:00,  2.01it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  99.49     
Total input tokens:                      202314    
Total generated tokens:                  1200      
Request throughput (req/s):              2.01      
Output token throughput (tok/s):         12.06     
Total Token throughput (tok/s):          2045.51   
---------------Time to First Token----------------
Mean TTFT (ms):                          1925.52   
Median TTFT (ms):                        1011.58   
P99 TTFT (ms):                           8149.52   
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          100.98    
Median TPOT (ms):                        13.64     
P99 TPOT (ms):                           957.56    
---------------Inter-token Latency----------------
Mean ITL (ms):                           100.73    
Median ITL (ms):                         12.42     
P99 ITL (ms):                            2536.78   
==================================================
```

## Short prefill length with varying request rates: input_len=256, qps = 2, 4, 6, 8

### Non-disaggregated
```
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-2.json --request-rate 2
WARNING 11-07 11:33:15 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=2.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-2.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 2.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [01:38<00:00,  2.03it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  98.72     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              2.03      
Output token throughput (tok/s):         12.16     
Total Token throughput (tok/s):          509.06    
---------------Time to First Token----------------
Mean TTFT (ms):                          63.68     
Median TTFT (ms):                        55.86     
P99 TTFT (ms):                           134.96    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          14.87     
Median TPOT (ms):                        12.19     
P99 TPOT (ms):                           39.43     
---------------Inter-token Latency----------------
Mean ITL (ms):                           14.85     
Median ITL (ms):                         12.17     
P99 ITL (ms):                            59.28     
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-4.json --request-rate 4
WARNING 11-07 11:35:08 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=4.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-4.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 4.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:49<00:00,  4.04it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  49.46     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              4.04      
Output token throughput (tok/s):         24.26     
Total Token throughput (tok/s):          1016.08   
---------------Time to First Token----------------
Mean TTFT (ms):                          74.93     
Median TTFT (ms):                        64.54     
P99 TTFT (ms):                           197.32    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          19.09     
Median TPOT (ms):                        12.27     
P99 TPOT (ms):                           84.70     
---------------Inter-token Latency----------------
Mean ITL (ms):                           19.04     
Median ITL (ms):                         12.25     
P99 ITL (ms):                            121.41    
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-6.json --request-rate 6
WARNING 11-07 11:36:12 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=6.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-6.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 6.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:33<00:00,  6.04it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  33.10     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              6.04      
Output token throughput (tok/s):         36.26     
Total Token throughput (tok/s):          1518.46   
---------------Time to First Token----------------
Mean TTFT (ms):                          91.94     
Median TTFT (ms):                        74.35     
P99 TTFT (ms):                           269.85    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          27.47     
Median TPOT (ms):                        16.11     
P99 TPOT (ms):                           121.31    
---------------Inter-token Latency----------------
Mean ITL (ms):                           27.40     
Median ITL (ms):                         12.35     
P99 ITL (ms):                            242.38    
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-8.json --request-rate 8
WARNING 11-07 11:36:59 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=8.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-8.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 8.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:24<00:00,  8.02it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  24.94     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              8.02      
Output token throughput (tok/s):         48.12     
Total Token throughput (tok/s):          2015.39   
---------------Time to First Token----------------
Mean TTFT (ms):                          120.43    
Median TTFT (ms):                        88.21     
P99 TTFT (ms):                           397.35    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          42.95     
Median TPOT (ms):                        24.63     
P99 TPOT (ms):                           182.10    
---------------Inter-token Latency----------------
Mean ITL (ms):                           42.87     
Median ITL (ms):                         12.55     
P99 ITL (ms):                            561.46    
==================================================
```

### Non-disaggregated with `--enable-chunked-prefill` enabled
```
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-2.json --request-rate 2
WARNING 11-06 15:10:16 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=2.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-2.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 2.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [01:38<00:00,  2.03it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  98.72     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              2.03      
Output token throughput (tok/s):         12.16     
Total Token throughput (tok/s):          509.05    
---------------Time to First Token----------------
Mean TTFT (ms):                          64.55     
Median TTFT (ms):                        56.19     
P99 TTFT (ms):                           136.14    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          14.29     
Median TPOT (ms):                        12.20     
P99 TPOT (ms):                           34.41     
---------------Inter-token Latency----------------
Mean ITL (ms):                           14.27     
Median ITL (ms):                         12.21     
P99 ITL (ms):                            45.76     
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-4.json --request-rate 4
WARNING 11-06 15:12:09 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=4.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-4.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 4.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:49<00:00,  4.04it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  49.45     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              4.04      
Output token throughput (tok/s):         24.27     
Total Token throughput (tok/s):          1016.31   
---------------Time to First Token----------------
Mean TTFT (ms):                          76.65     
Median TTFT (ms):                        65.66     
P99 TTFT (ms):                           185.25    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          17.46     
Median TPOT (ms):                        12.25     
P99 TPOT (ms):                           58.56     
---------------Inter-token Latency----------------
Mean ITL (ms):                           17.44     
Median ITL (ms):                         12.27     
P99 ITL (ms):                            69.81     
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-6.json --request-rate 6
WARNING 11-06 15:13:13 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=6.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-6.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 6.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:33<00:00,  6.04it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  33.10     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              6.04      
Output token throughput (tok/s):         36.25     
Total Token throughput (tok/s):          1518.19   
---------------Time to First Token----------------
Mean TTFT (ms):                          92.19     
Median TTFT (ms):                        76.48     
P99 TTFT (ms):                           253.62    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          22.06     
Median TPOT (ms):                        16.43     
P99 TPOT (ms):                           77.50     
---------------Inter-token Latency----------------
Mean ITL (ms):                           22.04     
Median ITL (ms):                         12.34     
P99 ITL (ms):                            127.60    
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-8.json --request-rate 8
WARNING 11-06 15:14:00 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=8.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-8.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 8.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:24<00:00,  8.02it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  24.93     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              8.02      
Output token throughput (tok/s):         48.13     
Total Token throughput (tok/s):          2015.49   
---------------Time to First Token----------------
Mean TTFT (ms):                          120.73    
Median TTFT (ms):                        91.26     
P99 TTFT (ms):                           340.52    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          33.93     
Median TPOT (ms):                        22.25     
P99 TPOT (ms):                           131.26    
---------------Inter-token Latency----------------
Mean ITL (ms):                           33.91     
Median ITL (ms):                         12.56     
P99 ITL (ms):                            151.47    
==================================================
```


### MooncakeTransferEngine with RDMA backend
```
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-2.json --request-rate 2
WARNING 11-06 14:28:17 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=2.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-2.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 2.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [01:38<00:00,  2.03it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  98.70     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              2.03      
Output token throughput (tok/s):         12.16     
Total Token throughput (tok/s):          509.14    
---------------Time to First Token----------------
Mean TTFT (ms):                          103.54    
Median TTFT (ms):                        89.44     
P99 TTFT (ms):                           223.15    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          12.02     
Median TPOT (ms):                        11.49     
P99 TPOT (ms):                           16.28     
---------------Inter-token Latency----------------
Mean ITL (ms):                           11.98     
Median ITL (ms):                         11.46     
P99 ITL (ms):                            30.30     
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-4.json --request-rate 4
WARNING 11-06 14:30:14 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=4.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-4.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 4.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:49<00:00,  4.04it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  49.48     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              4.04      
Output token throughput (tok/s):         24.25     
Total Token throughput (tok/s):          1015.58   
---------------Time to First Token----------------
Mean TTFT (ms):                          122.52    
Median TTFT (ms):                        91.97     
P99 TTFT (ms):                           300.48    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          13.30     
Median TPOT (ms):                        11.52     
P99 TPOT (ms):                           40.96     
---------------Inter-token Latency----------------
Mean ITL (ms):                           13.27     
Median ITL (ms):                         11.48     
P99 ITL (ms):                            54.24     
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-6.json --request-rate 6
WARNING 11-06 14:31:18 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=6.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-6.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 6.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:33<00:00,  6.03it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  33.15     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              6.03      
Output token throughput (tok/s):         36.20     
Total Token throughput (tok/s):          1515.84   
---------------Time to First Token----------------
Mean TTFT (ms):                          155.67    
Median TTFT (ms):                        118.36    
P99 TTFT (ms):                           629.47    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          16.28     
Median TPOT (ms):                        11.62     
P99 TPOT (ms):                           57.13     
---------------Inter-token Latency----------------
Mean ITL (ms):                           16.23     
Median ITL (ms):                         11.53     
P99 ITL (ms):                            116.64    
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-8.json --request-rate 8
WARNING 11-06 14:32:05 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=8.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-8.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 8.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:24<00:00,  8.00it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  25.00     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              8.00      
Output token throughput (tok/s):         48.01     
Total Token throughput (tok/s):          2010.42   
---------------Time to First Token----------------
Mean TTFT (ms):                          238.67    
Median TTFT (ms):                        154.85    
P99 TTFT (ms):                           937.64    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          24.58     
Median TPOT (ms):                        12.26     
P99 TPOT (ms):                           172.75    
---------------Inter-token Latency----------------
Mean ITL (ms):                           24.51     
Median ITL (ms):                         11.61     
P99 ITL (ms):                            301.37    
==================================================
```

### MooncakeTransferEngine with multiple RDMA NIC devices
```
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-2.json --request-rate 2
WARNING 11-11 16:27:38 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=2.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-2.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 2.0
100%|███████████████████████████████████████████████████████████████████████| 200/200 [01:38<00:00,  2.03it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  98.71     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              2.03      
Output token throughput (tok/s):         12.16     
Total Token throughput (tok/s):          509.11    
---------------Time to First Token----------------
Mean TTFT (ms):                          101.80    
Median TTFT (ms):                        88.78     
P99 TTFT (ms):                           220.65    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          11.98     
Median TPOT (ms):                        11.46     
P99 TPOT (ms):                           16.68     
---------------Inter-token Latency----------------
Mean ITL (ms):                           11.95     
Median ITL (ms):                         11.44     
P99 ITL (ms):                            30.70     
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-4.json --request-rate 4
WARNING 11-11 16:29:31 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=4.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-4.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 4.0
100%|███████████████████████████████████████████████████████████████████████| 200/200 [00:49<00:00,  4.04it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  49.50     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              4.04      
Output token throughput (tok/s):         24.24     
Total Token throughput (tok/s):          1015.19   
---------------Time to First Token----------------
Mean TTFT (ms):                          122.88    
Median TTFT (ms):                        90.74     
P99 TTFT (ms):                           303.25    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          13.33     
Median TPOT (ms):                        11.52     
P99 TPOT (ms):                           42.81     
---------------Inter-token Latency----------------
Mean ITL (ms):                           13.30     
Median ITL (ms):                         11.48     
P99 ITL (ms):                            55.40     
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-6.json --request-rate 6
WARNING 11-11 16:30:35 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=6.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-6.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 6.0
100%|███████████████████████████████████████████████████████████████████████| 200/200 [00:33<00:00,  6.03it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  33.15     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              6.03      
Output token throughput (tok/s):         36.20     
Total Token throughput (tok/s):          1515.83   
---------------Time to First Token----------------
Mean TTFT (ms):                          151.39    
Median TTFT (ms):                        118.74    
P99 TTFT (ms):                           469.07    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          15.97     
Median TPOT (ms):                        11.61     
P99 TPOT (ms):                           72.20     
---------------Inter-token Latency----------------
Mean ITL (ms):                           15.93     
Median ITL (ms):                         11.53     
P99 ITL (ms):                            116.50    
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-8.json --request-rate 8
WARNING 11-11 16:31:23 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=8.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-8.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 8.0
100%|███████████████████████████████████████████████████████████████████████| 200/200 [00:24<00:00,  8.00it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  24.99     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              8.00      
Output token throughput (tok/s):         48.02     
Total Token throughput (tok/s):          2011.02   
---------------Time to First Token----------------
Mean TTFT (ms):                          241.09    
Median TTFT (ms):                        155.78    
P99 TTFT (ms):                           959.04    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          23.43     
Median TPOT (ms):                        12.27     
P99 TPOT (ms):                           196.75    
---------------Inter-token Latency----------------
Mean ITL (ms):                           23.35     
Median ITL (ms):                         11.62     
P99 ITL (ms):                            323.14    
==================================================
```

### MooncakeTransferEngine with TCP backend
```
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-2.json --request-rate 2
WARNING 11-06 13:51:12 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=2.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-2.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 2.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [01:38<00:00,  2.03it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  98.71     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              2.03      
Output token throughput (tok/s):         12.16     
Total Token throughput (tok/s):          509.09    
---------------Time to First Token----------------
Mean TTFT (ms):                          111.86    
Median TTFT (ms):                        95.78     
P99 TTFT (ms):                           241.02    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          12.14     
Median TPOT (ms):                        11.45     
P99 TPOT (ms):                           17.66     
---------------Inter-token Latency----------------
Mean ITL (ms):                           12.11     
Median ITL (ms):                         11.44     
P99 ITL (ms):                            36.33     
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-4.json --request-rate 4
WARNING 11-06 13:53:05 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=4.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-4.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 4.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:49<00:00,  4.04it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  49.50     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              4.04      
Output token throughput (tok/s):         24.24     
Total Token throughput (tok/s):          1015.29   
---------------Time to First Token----------------
Mean TTFT (ms):                          132.38    
Median TTFT (ms):                        99.81     
P99 TTFT (ms):                           329.00    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          14.50     
Median TPOT (ms):                        11.51     
P99 TPOT (ms):                           77.32     
---------------Inter-token Latency----------------
Mean ITL (ms):                           14.47     
Median ITL (ms):                         11.49     
P99 ITL (ms):                            77.08     
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-6.json --request-rate 6
WARNING 11-06 13:54:09 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=6.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-6.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 6.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:33<00:00,  6.03it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  33.17     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              6.03      
Output token throughput (tok/s):         36.18     
Total Token throughput (tok/s):          1515.03   
---------------Time to First Token----------------
Mean TTFT (ms):                          173.83    
Median TTFT (ms):                        134.61    
P99 TTFT (ms):                           692.35    
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          19.74     
Median TPOT (ms):                        11.62     
P99 TPOT (ms):                           99.98     
---------------Inter-token Latency----------------
Mean ITL (ms):                           19.69     
Median ITL (ms):                         11.53     
P99 ITL (ms):                            140.50    
==================================================
+ python3 ../benchmark_serving.py --backend vllm --model Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 --dataset-name sonnet --dataset-path ../sonnet_4x.txt --sonnet-input-len 256 --sonnet-output-len 6 --sonnet-prefix-len 50 --num-prompts 200 --port 8000 --save-result --result-dir ./results --result-filename disagg_prefill-qps-8.json --request-rate 8
WARNING 11-06 13:54:56 cuda.py:22] You are using a deprecated `pynvml` package. Please install `nvidia-ml-py` instead, and make sure to uninstall `pynvml`. When both of them are installed, `pynvml` will take precedence and cause errors. See https://pypi.org/project/pynvml for more information.
Namespace(backend='vllm', base_url=None, host='localhost', port=8000, endpoint='/v1/completions', dataset=None, dataset_name='sonnet', dataset_path='../sonnet_4x.txt', model='Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4', tokenizer=None, best_of=1, use_beam_search=False, num_prompts=200, logprobs=None, request_rate=8.0, seed=0, trust_remote_code=False, disable_tqdm=False, profile=False, save_result=True, metadata=None, result_dir='./results', result_filename='disagg_prefill-qps-8.json', ignore_eos=False, percentile_metrics='ttft,tpot,itl', metric_percentiles='99', sonnet_input_len=256, sonnet_output_len=6, sonnet_prefix_len=50, sharegpt_output_len=None, random_input_len=1024, random_output_len=128, random_range_ratio=1.0, random_prefix_len=0, hf_subset=None, hf_split=None, hf_output_len=None)
Starting initial single prompt test run...
Initial test run completed. Starting main benchmark run...
Traffic request rate: 8.0
100%|███████████████████████████████████████████████████████████████████████████| 200/200 [00:25<00:00,  8.00it/s]
============ Serving Benchmark Result ============
Successful requests:                     200       
Benchmark duration (s):                  25.02     
Total input tokens:                      49054     
Total generated tokens:                  1200      
Request throughput (req/s):              8.00      
Output token throughput (tok/s):         47.97     
Total Token throughput (tok/s):          2008.92   
---------------Time to First Token----------------
Mean TTFT (ms):                          282.11    
Median TTFT (ms):                        170.38    
P99 TTFT (ms):                           1168.48   
-----Time per Output Token (excl. 1st token)------
Mean TPOT (ms):                          25.57     
Median TPOT (ms):                        12.43     
P99 TPOT (ms):                           211.11    
---------------Inter-token Latency----------------
Mean ITL (ms):                           25.48     
Median ITL (ms):                         11.69     
P99 ITL (ms):                            415.90    
==================================================
```
