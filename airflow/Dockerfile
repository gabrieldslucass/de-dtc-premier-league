ARG BASE_AIRFLOW_IMAGE
FROM apache/airflow:2.10.2

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update -qq && apt-get install -y python3 python3-pip

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=456.0.0
ENV GCLOUD_HOME=/opt/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --additional-components alpha beta kubectl \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && rm -rf "${GCLOUD_HOME}/.install/.backup/" \
    && gcloud --version

ENV JAVA_HOME=/home/jdk-11.0.2

ENV PATH="${JAVA_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && mkdir -p "${JAVA_HOME}" \
    && tar xzf "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" -C "${JAVA_HOME}" --strip-components=1 \
    && rm -rf "${TMP_DIR}" \
    && java --version

COPY requirements.txt .

RUN python3 -m pip install --upgrade pip

RUN python3 -m pip install --no-cache-dir -r requirements.txt

USER ${AIRFLOW_UID}