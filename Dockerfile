FROM rust:latest AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y cmake
COPY Cargo.toml Cargo.lock /build
COPY src /build/src/
ARG PROFILE=release
ENV PROFILE=$PROFILE
RUN cargo build --verbose --profile ${PROFILE} \
 && cargo test --verbose --profile ${PROFILE}

FROM scratch
ARG PROFILE=release
ENV PROFILE=$PROFILE
COPY --from=builder /build/target/${PROFILE}/piproxy /
ENTRYPOINT ["/piproxy"]
