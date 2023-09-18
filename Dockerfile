FROM golang:1.20.5 AS builder
COPY . /app
WORKDIR /app
RUN CGO_ENABLED=0 GOOS=linux go build ./cmd/ccloud-schema-exporter/ccloud-schema-exporter.go

FROM scratch
COPY --from=builder /app/ccloud-schema-exporter /
ADD cmd/trustedEntities /etc/ssl/certs/
ENTRYPOINT ["/ccloud-schema-exporter", "-sync", "-syncDeletes", "-syncHardDeletes", "-withMetrics", "-noPrompt"]