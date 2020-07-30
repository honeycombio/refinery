FROM golang:1.14-alpine AS build

RUN apk update && apk upgrade && \
  apk add --no-cache git

WORKDIR /tmp/app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN go build -o ./out/samproxy ./cmd/samproxy

FROM alpine:latest

RUN apk add ca-certificates

COPY --from=build /tmp/app/out/samproxy /app/samproxy

WORKDIR "/app"

EXPOSE 8080
EXPOSE 8081

CMD ["./samproxy", "-c=./config.toml", "-r=./rules.toml", "-p", "redis"]
