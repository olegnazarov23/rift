# tiny_http (vendored)

[tiny_http](https://github.com/tiny-http/tiny-http) 0.12.0 from crates.io, MIT or Apache-2.0
(LICENSE-MIT, LICENSE-APACHE). Its last release pins rustls 0.20, which has published advisories
(RUSTSEC-2024-0336: a network peer can make a TLS connection loop forever; RUSTSEC-2026-0349) and
depends on unmaintained crates (ring 0.16, rustls-pemfile 0.2). Changes for rift:

- `src/ssl/rustls.rs` uses rustls 0.23 (with the ring provider, chosen explicitly) and reads PEM
  with rustls-pki-types instead of rustls-pemfile; a bad key is an error, not a panic.
- `Cargo.toml` lists the new dependencies and drops OpenSSL support, tests, benches and examples.

Changed files say so in their first line.
