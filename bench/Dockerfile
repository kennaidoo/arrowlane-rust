FROM rust:1.99.1-bookworm AS build

ENV RUSTUP_TOOLCHAIN=1.99.1

WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p arrowlane --bin arrowlane \
 && mkdir -p /out \
 && cp target/release/arrowlane /out/arrowlane

FROM gcr.io/distroless/cc-debian12

COPY --from=build /out/arrowlane /arrowlane
USER 65532
ENTRYPOINT ["/arrowlane"]
