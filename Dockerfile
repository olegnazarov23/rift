# rift: built in the Rust image, shipped on distroless (glibc and CA roots; no shell or package
# manager). Base images are pinned by digest; Dependabot proposes updates.
FROM rust:1.99-slim-bookworm@sha256:f2c004e3d2d8b547e8cf7ffbc29205385f4e71d23e53e4e383065db5e9f822cd AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY vendor vendor
COPY src src
RUN cargo build --release --locked && strip target/release/rift

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
LABEL org.opencontainers.image.source="https://github.com/olegnazarov23/rift" \
      org.opencontainers.image.description="Byte-exact recompression for container registries" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /src/target/release/rift /usr/local/bin/rift
# cold reads are rebuilt into $TMPDIR while clients download them (and hot copies made from there);
# give it room for your largest layers, above --max-spill-gb
ENV TMPDIR=/tmp
USER 65532:65532
EXPOSE 9100
ENTRYPOINT ["/usr/local/bin/rift"]
CMD ["gateway", "--listen", "0.0.0.0:9100"]
