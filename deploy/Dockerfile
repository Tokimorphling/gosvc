# syntax=docker/dockerfile:1

FROM golang:1.27-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_TIME=unknown
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w \
      -X example.com/gosvc/examples/app.Version=${VERSION} \
      -X example.com/gosvc/examples/app.Commit=${COMMIT} \
      -X example.com/gosvc/examples/app.BuildTime=${BUILD_TIME}" \
    -o /out/gosvc ./cmd/gosvc

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app

COPY --from=build /out/gosvc /app/gosvc
COPY configs/config.example.toml /app/config.toml

# HTTP, gRPC, admin (metrics/pprof)
EXPOSE 8080 9090 6060

USER nonroot:nonroot
ENTRYPOINT ["/app/gosvc"]
CMD ["-c", "/app/config.toml"]
