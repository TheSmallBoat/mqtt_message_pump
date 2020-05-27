FROM alpine:3.8

LABEL maintainer="Abe.Chua <abecai@gmail.com>" \
    description="iotstream pump plus"

ENV VERSION=2020.01.28

RUN set -x && \
    addgroup -S -g 1883 iotstream 2>/dev/null && \
    adduser -S -u 1883 -D -H -h /var/empty -s /sbin/nologin -G iotstream -g iotstream iotstream 2>/dev/null && \
    mkdir -p /iotstream/config && \
    chown -R iotstream:iotstream /iotstream

WORKDIR /iotstream
# Set up the entry point script and default command

COPY docker-entrypoint.sh /
COPY pump-plus-linux ./
COPY conf/pump-plus-emqx-example.ini ./config/pump-plus.ini

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["./pump-plus-linux", "run", "-c", "./config/pump-plus.ini"]