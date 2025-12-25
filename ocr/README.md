uv run vllm serve allenai/olmOCR-2-7B-1025-FP8 \
  --max-model-len 16384 \
  --gpu-memory-utilization 0.9 \
  --max-num-seqs 32 \
  --no-enable-prefix-caching \
  --enable-chunked-prefill