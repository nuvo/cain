FROM alpine:3.8
RUN apk --no-cache add ca-certificates
COPY cain /usr/local/bin/cain
RUN addgroup -g 1001 -S cain \
    && adduser -u 1001 -D -S -G cain cain
USER cain
WORKDIR /home/cain
CMD ["cain"]
