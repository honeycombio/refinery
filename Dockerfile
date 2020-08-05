FROM golang:1.14

ARG BUILD_ID=dev

WORKDIR /app

ADD go.mod go.sum ./

RUN go mod download
RUN go mod verify

ADD . .

RUN CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    go build -ldflags "-X main.BuildID=${BUILD_ID}}" ./cmd/samproxy

FROM alpine

RUN apk add --update --no-cache ca-certificates
COPY --from=0 /app/samproxy /usr/bin/samproxy
