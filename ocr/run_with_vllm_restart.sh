#!/bin/bash
# VLLM Restart Wrapper for Long-Running Batch Jobs
# ================================================
# This script processes batches and restarts VLLM between them to prevent memory leaks.
# Only use this if you've confirmed VLLM is the source of memory growth.
#
# Usage:
#   chmod +x ocr/run_with_vllm_restart.sh
#   ./ocr/run_with_vllm_restart.sh

set -e

MODEL="allenai/olmOCR-2-7B-1025-FP8"
MAX_MODEL_LEN=16384
GPU_MEM=0.9
VLLM_PORT=8000
BATCH_SIZE=100  # Process this many files, then restart VLLM

echo "Starting VLLM + OCR processing with periodic VLLM restarts..."
echo "Press Ctrl+C to stop"
echo ""

# Function to start VLLM
start_vllm() {
    echo "Starting VLLM server..."
    uv run vllm serve "$MODEL" \
        --max-model-len "$MAX_MODEL_LEN" \
        --gpu-memory-utilization "$GPU_MEM" \
        --enable-chunked-prefill \
        --port "$VLLM_PORT" &
    
    VLLM_PID=$!
    echo "VLLM started with PID: $VLLM_PID"
    
    # Wait for VLLM to be ready
    echo "Waiting for VLLM to be ready..."
    sleep 30
    
    # Check if server is responding
    for i in {1..10}; do
        if curl -s "http://localhost:$VLLM_PORT/health" > /dev/null 2>&1; then
            echo "VLLM is ready!"
            return 0
        fi
        echo "Waiting for VLLM... ($i/10)"
        sleep 5
    done
    
    echo "ERROR: VLLM failed to start or is not responding"
    return 1
}

# Function to stop VLLM
stop_vllm() {
    echo "Stopping VLLM (PID: $VLLM_PID)..."
    kill $VLLM_PID 2>/dev/null || true
    wait $VLLM_PID 2>/dev/null || true
    
    # Make sure it's really dead
    sleep 5
    pkill -9 -f "vllm.*$MODEL" 2>/dev/null || true
    echo "VLLM stopped"
}

# Trap Ctrl+C to clean up
trap 'echo "Interrupted!"; stop_vllm; exit 1' INT TERM

# Main loop
ITERATION=1
MAX_ITERATIONS=100  # Safety limit

while [ $ITERATION -le $MAX_ITERATIONS ]; do
    echo ""
    echo "========================================"
    echo "Iteration $ITERATION - Starting VLLM"
    echo "========================================"
    
    # Start VLLM
    if ! start_vllm; then
        echo "Failed to start VLLM, exiting"
        exit 1
    fi
    
    # Run OCR batch
    echo ""
    echo "Running OCR batch..."
    python ocr/run_batch_ocr.py
    OCR_EXIT_CODE=$?
    
    # Stop VLLM
    stop_vllm
    
    # Check if OCR finished successfully
    if [ $OCR_EXIT_CODE -ne 0 ]; then
        echo "OCR script exited with error code $OCR_EXIT_CODE"
        exit $OCR_EXIT_CODE
    fi
    
    # Check if all files are processed (OCR script would have reported this)
    echo ""
    echo "Checking if more files need processing..."
    # You could add a check here, for now we'll just continue
    
    # Clean system memory
    echo "Clearing system caches..."
    sync
    
    echo ""
    echo "Iteration $ITERATION complete. Waiting 10 seconds before next iteration..."
    sleep 10
    
    ITERATION=$((ITERATION + 1))
done

echo ""
echo "All iterations complete!"

