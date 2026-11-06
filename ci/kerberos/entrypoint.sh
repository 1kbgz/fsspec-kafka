#!/bin/sh
# Create the FSSPEC.TEST realm, the broker and client principals, and their keytabs.
set -e
rm -f /keytabs/*.keytab
mkdir -p /var/lib/krb5kdc
kdb5_util create -s -r FSSPEC.TEST -P fsspec-master
kadmin.local -q "addprinc -randkey kafka/localhost@FSSPEC.TEST"
kadmin.local -q "addprinc -randkey client@FSSPEC.TEST"
kadmin.local -q "ktadd -k /keytabs/kafka.keytab kafka/localhost@FSSPEC.TEST"
kadmin.local -q "ktadd -k /keytabs/client.keytab client@FSSPEC.TEST"
chmod 644 /keytabs/*.keytab
exec krb5kdc -n
