FROM golang:1.17-bullseye AS builder

RUN apt update && apt install -y cmake libcurl4-openssl-dev postgresql-server-dev-all \
    libpq-dev rapidjson-dev unixodbc unixodbc-dev libsqlite3-dev

WORKDIR /usr/src/ldpmarc
COPY . /usr/src/ldpmarc

RUN chmod +x ./build.sh && ./build.sh

FROM debian:bullseye-slim

LABEL org.opencontainers.image.source https://github.com/library-data-platform/ldpmarc
ENV DATADIR=/var/lib/ldp

COPY --from=builder /usr/src/ldpmarc/bin/ldpmarc /usr/local/bin/ldpmarc
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN mkdir $DATADIR && \
    chmod +x /usr/local/bin/docker-entrypoint.sh

VOLUME $DATADIR

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

