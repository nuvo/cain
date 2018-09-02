FROM golang:1.10.3-alpine as builder
WORKDIR /go/src/github.com/maorfr/cain/
COPY . .
RUN apk --no-cache add git glide \
    && glide up \
    && CGO_ENABLED=0 GOOS=linux go build -o cain cmd/cain.go

FROM alpine:3.8
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /go/src/github.com/maorfr/cain/cain /usr/local/bin/cain
CMD ["cain"]
