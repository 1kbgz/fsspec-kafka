FROM docker.io/library/alpine:3.22
RUN apk add --no-cache krb5-server krb5
COPY krb5-broker.conf /etc/krb5.conf
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
