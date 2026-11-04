# syntax=docker/dockerfile:1
ARG GO_VERSION=1.27.2
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN mkdir /out && for app in offline replayer worker master; do CGO_ENABLED=0 go build -trimpath -mod=readonly -o /out/$app ./cmd/$app; done && CGO_ENABLED=0 go build -trimpath -mod=readonly -o /out/bootstrap ./deploy/bootstrap
FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -S cad && adduser -S -G cad cad
COPY --from=build /out/ /app/
COPY deploy/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
USER cad
WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]
