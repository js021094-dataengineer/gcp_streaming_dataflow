# One image used for BOTH the Flex Template launcher and the Dataflow workers
# (sdk_container_image), so workers start with every dependency preinstalled.
ARG BEAM_VERSION=2.75.0
ARG PY_VERSION=311
# Pinned: `latest` moved to a new release on 2026-10-02 and its layers failed to unpack in Cloud Build
# ("invalid tar header"). This is the release the first successful build used. Bump deliberately.
ARG LAUNCHER_TAG=20260901-rc00

FROM gcr.io/dataflow-templates-base/python${PY_VERSION}-template-launcher-base:${LAUNCHER_TAG} AS template_launcher
FROM apache/beam_python3.11_sdk:${BEAM_VERSION}

# The Storage Write API is a cross-language (Java) transform in the Python SDK.
# The launcher expands it at job-submission time, which needs a JRE.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

WORKDIR /template
COPY requirements.txt setup.py main.py ./
COPY crypto_pipeline ./crypto_pipeline
RUN pip install --no-cache-dir -r requirements.txt && pip install --no-cache-dir .

# Pre-fetch the BigQuery expansion-service jar so job launches don't download it.
RUN python -c "from apache_beam.utils.subprocess_server import JavaJarServer as J; \
print(J.local_jar(J.path_to_beam_jar('sdks:java:io:google-cloud-platform:expansion-service:build')))" \
    || echo 'jar prefetch skipped; it will be downloaded at launch time'

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/main.py
# Dependencies are baked in: tell the launcher not to pip-install anything at runtime.
ENV PIP_NO_DEPS=True

# Keep the Beam SDK harness entrypoint so this image also works as worker image.
ENTRYPOINT ["/opt/apache/beam/boot"]
