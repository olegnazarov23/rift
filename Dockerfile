# rift: built in the Rust image, shipped in slim Debian
FROM rust:1.99-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY vendor vendor
COPY src src
RUN cargo build --release --locked && strip target/release/rift

FROM debian:bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/olegnazarov23/rift" \
      org.opencontainers.image.description="Byte-exact recompression for container registries" \
      org.opencontainers.image.licenses="Apache-2.0"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/rift /usr/local/bin/rift
# cold reads are rebuilt into $TMPDIR while clients download them (and hot copies made from there);
# give it room for your largest layers, above --max-spill-gb
ENV TMPDIR=/tmp
USER 65532:65532
EXPOSE 9100
ENTRYPOINT ["rift"]
CMD ["gateway", "--listen", "0.0.0.0:9100"]
