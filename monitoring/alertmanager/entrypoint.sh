#!/bin/sh
# Renders Gmail values from the environment into the mounted template.
set -eu
: "${ALERTMANAGER_SMTP_FROM:?missing ALERTMANAGER_SMTP_FROM}"
: "${ALERTMANAGER_SMTP_AUTH_USERNAME:?missing ALERTMANAGER_SMTP_AUTH_USERNAME}"
: "${ALERTMANAGER_SMTP_AUTH_PASSWORD:?missing ALERTMANAGER_SMTP_AUTH_PASSWORD}"
: "${ALERTMANAGER_RECEIVER_EMAIL:?missing ALERTMANAGER_RECEIVER_EMAIL}"
escape() {
    printf '%s' "$1" | sed -e 's/[\\/&]/\\&/g'
}
cp /etc/alertmanager/alertmanager.yml /tmp/alertmanager.yml
for key in ALERTMANAGER_SMTP_FROM ALERTMANAGER_SMTP_AUTH_USERNAME ALERTMANAGER_SMTP_AUTH_PASSWORD ALERTMANAGER_RECEIVER_EMAIL; do
    eval "value=\${$key}"
    sed -i -e "s|\${$key}|$(escape "$value")|g" /tmp/alertmanager.yml
done
exec /bin/alertmanager --config.file=/tmp/alertmanager.yml "$@"
