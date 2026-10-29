FROM rust:1.99.1-bookworm AS build

WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
RUN cargo build --release -p arrowlane --bin arrowlane

FROM debian:bookworm-slim

RUN useradd --uid 65532 --no-create-home bench
COPY --from=build /src/target/release/arrowlane /usr/local/bin/arrowlane
USER 65532

ENTRYPOINT ["/usr/local/bin/arrowlane"]
