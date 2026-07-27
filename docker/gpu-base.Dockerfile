ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC \
    PYTHONUNBUFFERED=1 \
    NVIDIA_VISIBLE_DEVICES=all \
    NVIDIA_DRIVER_CAPABILITIES=compute,utility

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# cu128 supports the driver versions available across the worker fleet
RUN pip install --no-cache-dir \
    "torch==2.9.1+cu128" \
    "torchvision==0.24.1+cu128" \
    "torchaudio==2.9.1+cu128" \
    --extra-index-url https://download.pytorch.org/whl/cu128
