FROM golang:1.26-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /redpanda-provisioner ./cmd/redpanda-provisioner

FROM scratch
COPY --from=builder /redpanda-provisioner /redpanda-provisioner
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["/redpanda-provisioner"]
