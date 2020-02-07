FROM golang:alpine

RUN apk add --update --no-cache git
RUN go get github.com/honeycombio/samproxy/cmd/samproxy

FROM alpine

RUN apk add --update --no-cache ca-certificates
COPY --from=0 /go/bin/samproxy /usr/bin/samproxy

