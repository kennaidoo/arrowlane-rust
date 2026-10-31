FROM rust:1.99.1-bookworm AS build

ENV RUSTUP_TOOLCHAIN=1.99.1

WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p arrowlane --bin arrowlane \
 && mkdir -p /out/root \
 && cp target/release/arrowlane /out/root/arrowlane \
 && for lib in $(ldd target/release/arrowlane | awk '/=> \// {print $3}'); do \
      mkdir -p "/out/root$(dirname "$lib")"; \
      cp "$lib" "/out/root$lib"; \
    done \
 && loader=$(ldd target/release/arrowlane | awk '/ld-linux/ {print $1}') \
 && mkdir -p "/out/root$(dirname "$loader")" \
 && cp "$loader" "/out/root$loader"

FROM scratch

COPY --from=build /out/root/ /
USER 65532
ENTRYPOINT ["/arrowlane"]
