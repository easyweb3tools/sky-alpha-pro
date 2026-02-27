FROM golang:1.23-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN /bin/sh -c 'for i in 1 2 3; do go mod download && exit 0; sleep 2; done; exit 1'

COPY . .

ARG APP_VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o /out/sky-alpha-pro ./cmd/sky-alpha-pro

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /out/sky-alpha-pro /usr/local/bin/sky-alpha-pro
COPY configs/config.yaml /app/configs/config.yaml

ARG APP_VERSION=dev
ENV SKY_ALPHA_APP_VERSION=${APP_VERSION}

ENTRYPOINT ["sky-alpha-pro"]
CMD ["serve", "--config", "/app/configs/config.yaml"]
