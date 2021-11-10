FROM debian:bookworm-slim as base

ARG BUILD_DATE
ARG VCS_REF

LABEL maintainer="Martin Abbrent <martin.abbrent@ufz.de>" \
    org.opencontainers.image.title="DJANGO Base Image" \
    org.opencontainers.image.licenses="HEESIL" \
    org.opencontainers.image.version="0.0.1" \
    org.opencontainers.image.revision=$VCS_REF \
    org.opencontainers.image.created=$BUILD_DATE

RUN apt-get -y update \
    && apt-get -y dist-upgrade \
    && apt-get -y --no-install-recommends install \
      python3 \
      libaio1 \
      ca-certificates \
    && apt-get -y autoremove \
    && apt-get -y autoclean \
    && rm -rf /var/lib/apt

FROM base as build

RUN apt-get -y update \
    && apt-get -y --no-install-recommends install \
      git openssh-client \
      python3-pip \
      curl \
      unzip

# Install minio mc client
RUN curl https://dl.min.io/client/mc/release/linux-amd64/mc > /usr/local/bin/mc \
    && chmod a+x /usr/local/bin/mc

# add requirements
COPY src/requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip \
    && pip install \
        --user \
        --no-cache-dir \
        --no-warn-script-location -r \
        /tmp/requirements.txt

FROM base as dist

# Create a group and user
RUN useradd --uid 1000 -m appuser

COPY --chown=appuser --from=build /root/.local /home/appuser/.local
COPY --from=build /usr/local/bin/mc /usr/local/bin/mc

# Tell docker that all future commands should run as the appuser user
USER appuser

WORKDIR /home/appuser/app/src

COPY src .

ENTRYPOINT ["python3", "main.py"]
