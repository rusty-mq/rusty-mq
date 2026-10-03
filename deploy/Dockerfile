# rusty-mq container image: multi-stage build, non-root runtime, no
# embedded credentials (PRD §13.3).
FROM rust:1.95-slim AS build
WORKDIR /src
# Lockfiles first for layer caching.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --bin rusty-mq

FROM debian:bookworm-slim
RUN groupadd -r rusty --gid=10001 && \
    useradd -r -g rusty --uid=10001 --home-dir /data --shell /usr/sbin/nologin rusty
COPY --from=build /src/target/release/rusty-mq /usr/local/bin/rusty-mq
COPY deploy/config.example.toml /etc/rusty-mq/config.example.toml
RUN mkdir -p /data && chown -R rusty:rusty /data /etc/rusty-mq
USER rusty
VOLUME ["/data"]
EXPOSE 5672 5671 15672 15692
ENTRYPOINT ["/usr/local/bin/rusty-mq"]
CMD ["serve", "--config", "/etc/rusty-mq/config.toml"]
