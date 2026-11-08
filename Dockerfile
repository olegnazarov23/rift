# rift: built in the Rust image, shipped in slim Debian
FROM rust:1.99-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY vendor vendor
COPY src src
RUN cargo build --release --locked && strip target/release/rift

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/rift /usr/local/bin/rift
# hot copies are written to $TMPDIR while a cold blob streams; give it room for your largest layer
ENV TMPDIR=/tmp
USER 65532:65532
EXPOSE 9100
ENTRYPOINT ["rift"]
CMD ["gateway", "--listen", "0.0.0.0:9100"]
