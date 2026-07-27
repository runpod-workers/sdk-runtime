ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}

ARG RUNPOD_PACKAGE_SPEC=runpod

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /tmp/sdk-runtime/
COPY src /tmp/sdk-runtime/src
RUN SETUPTOOLS_SCM_PRETEND_VERSION_FOR_RUNPOD=0.0.0.dev0 \
    pip install --no-cache-dir "${RUNPOD_PACKAGE_SPEC}" \
 && pip install --no-cache-dir /tmp/sdk-runtime \
 && rm -rf /tmp/sdk-runtime

EXPOSE 8080
CMD ["python", "-m", "runpod_sdk_runtime.task.runner"]
