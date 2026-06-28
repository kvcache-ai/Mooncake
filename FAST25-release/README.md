```json
{
    "timestamp": 27482,
    "input_length": 6955,
    "output_length": 52,
    "hash_ids": [46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 2353, 2354]
}
{
    "timestamp": 30535,
    "input_length": 6472,
    "output_length": 26,
    "hash_ids": [46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 2366]
}
```
The above presents two samples from our trace dataset. The trace includes the timing of request arrivals, the number of input tokens, the number of output tokens, and the remapped block hash. To protect our customers' privacy, we applied several mechanisms to remove user-related information while preserving the dataset's utility for simulated evaluation. More descriptions of the trace (e.g., up to 50% cache hit ratio) can be found in Section 4 of the technical report.

**_Update [Feb 21, 2025]: The updated [traces](./traces) used in our FAST'25 paper have been released! Please refer to the paper's appendix (found [here](Mooncake-FAST25.pdf)) for more details._**