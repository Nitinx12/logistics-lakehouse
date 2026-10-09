#!/usr/bin/env bash
# Creates or refreshes .env from .env.example with generated secrets.
set -euo pipefail

TEMPLATE_NAME=".env.example"
TARGET_NAME=".env"
ALCHEMY_KEY="AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
FORCE=0
CHANGED=0
CREATED=0
TMP_FILE=""
BACKUP_PATH=""

declare -A CURRENT
declare -A NEW
declare -A SEEN
declare -A GENERATOR_FOR=(
	[POSTGRES_PASSWORD]=gen_hex
	[POSTGRES_SUPERUSER_PASSWORD]=gen_hex
	[PG_ETL_PASSWORD]=gen_hex
	[PG_DQ_PASSWORD]=gen_hex
	[PG_ANALYST_PASSWORD]=gen_hex
	[PG_DASHBOARD_PASSWORD]=gen_hex
	[AIRFLOW_DB_PASSWORD]=gen_hex
	[AIRFLOW__WEBSERVER__SECRET_KEY]=gen_hex
	[_AIRFLOW_WWW_USER_PASSWORD]=gen_hex
	[REDIS_BROKER_PASSWORD]=gen_hex
	[REDIS_CACHE_PASSWORD]=gen_hex
	[STREAMLIT_PG_PASSWORD]=gen_hex
	[GRAFANA_ADMIN_PASSWORD]=gen_hex
	[AIRFLOW__CORE__FERNET_KEY]=gen_fernet_key
	[KAFKA_CLUSTER_ID]=gen_uuid
)

MANAGED_ORDER=(
	POSTGRES_PASSWORD
	POSTGRES_SUPERUSER_PASSWORD
	PG_ETL_PASSWORD
	PG_DQ_PASSWORD
	PG_ANALYST_PASSWORD
	PG_DASHBOARD_PASSWORD
	AIRFLOW_DB_PASSWORD
	AIRFLOW__WEBSERVER__SECRET_KEY
	_AIRFLOW_WWW_USER_PASSWORD
	REDIS_BROKER_PASSWORD
	REDIS_CACHE_PASSWORD
	STREAMLIT_PG_PASSWORD
	GRAFANA_ADMIN_PASSWORD
	AIRFLOW__CORE__FERNET_KEY
	KAFKA_CLUSTER_ID
)

APPEND_ORDER=(
	"${MANAGED_ORDER[@]}"
	"$ALCHEMY_KEY"
	AIRFLOW_DB_USER
	AIRFLOW_DB_NAME
)

GMAIL_PASSWORD_KEYS=(
	AIRFLOW__SMTP__SMTP_PASSWORD
	ALERTMANAGER_SMTP_AUTH_PASSWORD
)

GMAIL_ADDRESS_KEYS=(
	LAKEHOUSE_ALERT_EMAILS
	AIRFLOW__SMTP__SMTP_USER
	AIRFLOW__SMTP__SMTP_MAIL_FROM
	ALERTMANAGER_SMTP_FROM
	ALERTMANAGER_SMTP_AUTH_USERNAME
	ALERTMANAGER_RECEIVER_EMAIL
)

ROTATED=()
WARN=()

usage() {
	echo "usage: setup_env.sh [--force]"
	echo "Creates .env from .env.example and fills placeholder secrets."
	echo "--force regenerates every managed secret, even values already set."
}

require_cmd() {
	local name="$1"
	command -v "$name" >/dev/null 2>&1 || {
		echo "setup_env: required command not found: $name" >&2
		return 1
	}
}

gen_hex() {
	openssl rand -hex 32
}

gen_fernet_key() {
	openssl rand -base64 32 | tr -d '\r\n' | tr '+/' '-_'
}

gen_uuid() {
	local hex
	hex="$(openssl rand -hex 16)"
	printf '%s-%s-%s-%s-%s\n' "${hex:0:8}" "${hex:8:4}" "${hex:12:4}" "${hex:16:4}" "${hex:20:12}"
}

is_placeholder() {
	local value="$1"
	[[ "$value" == change_me* || "$value" == your_16_char_app_password || -z "$value" ]]
}

prompt_hidden() {
	local prompt="$1"
	local reply=""
	if [[ -t 0 ]]; then
		read -r -s -p "$prompt" reply || true
		printf '\n' >&2
	fi
	printf '%s' "$reply"
}

prompt_line() {
	local prompt="$1"
	local reply=""
	if [[ -t 0 ]]; then
		read -r -p "$prompt" reply || true
	fi
	printf '%s' "$reply"
}

note_if_changed() {
	local key="$1"
	local candidate="$2"
	NEW["$key"]="$candidate"
	if [[ "${CURRENT[$key]:-}" != "$candidate" ]]; then
		ROTATED+=("$key")
	fi
}

resolve_managed() {
	local key
	for key in "${MANAGED_ORDER[@]}"; do
		if (( FORCE )) || [[ -z "${CURRENT[$key]:-}" ]] || is_placeholder "${CURRENT[$key]}"; then
			note_if_changed "$key" "$("${GENERATOR_FOR[$key]}")"
		else
			NEW["$key"]="${CURRENT[$key]}"
		fi
	done
}

resolve_alchemy() {
	local db_user="${CURRENT[AIRFLOW_DB_USER]:-airflow}"
	local db_name="${CURRENT[AIRFLOW_DB_NAME]:-airflow}"
	local conn="postgresql+psycopg2://${db_user}:${NEW[AIRFLOW_DB_PASSWORD]}@airflow-metadata:5432/${db_name}"
	if (( FORCE )) || [[ -z "${CURRENT[$ALCHEMY_KEY]:-}" || "${CURRENT[$ALCHEMY_KEY]}" == *change_me* ]]; then
		note_if_changed "$ALCHEMY_KEY" "$conn"
	else
		NEW["$ALCHEMY_KEY"]="${CURRENT[$ALCHEMY_KEY]}"
	fi
	if [[ -z "${CURRENT[AIRFLOW_DB_USER]+x}" ]]; then
		NEW[AIRFLOW_DB_USER]="$db_user"
	fi
	if [[ -z "${CURRENT[AIRFLOW_DB_NAME]+x}" ]]; then
		NEW[AIRFLOW_DB_NAME]="$db_name"
	fi
}

resolve_gmail() {
	local key
	local need_password=0
	local need_address=0
	for key in "${GMAIL_PASSWORD_KEYS[@]}"; do
		if (( FORCE )) || [[ -z "${CURRENT[$key]:-}" ]] || is_placeholder "${CURRENT[$key]}"; then
			need_password=1
		fi
	done
	for key in "${GMAIL_ADDRESS_KEYS[@]}"; do
		if [[ "${CURRENT[$key]:-}" == "you@gmail.com" ]]; then
			need_address=1
		fi
	done
	if (( need_password )); then
		local app_password
		app_password="$(prompt_hidden "Gmail 16-char app password (empty to skip): ")"
		app_password="${app_password// /}"
		if [[ -n "$app_password" ]]; then
			for key in "${GMAIL_PASSWORD_KEYS[@]}"; do
				note_if_changed "$key" "$app_password"
			done
		else
			WARN+=("Gmail app password left as placeholder; alerts and report mail stay disabled until set")
		fi
	fi
	if (( need_address )); then
		local address
		address="$(prompt_line "Gmail address for alerts (empty to keep you@gmail.com): ")"
		if [[ -n "$address" ]]; then
			for key in "${GMAIL_ADDRESS_KEYS[@]}"; do
				if [[ "${CURRENT[$key]:-}" == "you@gmail.com" ]]; then
					note_if_changed "$key" "$address"
				fi
			done
		fi
	fi
}

load_current() {
	local target="$1"
	local line
	while IFS= read -r line || [[ -n "$line" ]]; do
		if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
			CURRENT["${BASH_REMATCH[1]}"]="${BASH_REMATCH[2]}"
		fi
	done < "$target"
}

rewrite_target() {
	local target="$1"
	local tmp="$2"
	local line
	local key
	while IFS= read -r line || [[ -n "$line" ]]; do
		if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.*)$ ]]; then
			key="${BASH_REMATCH[1]}"
			if [[ -n "${NEW[$key]+x}" ]]; then
				SEEN["$key"]=1
				if [[ "${NEW[$key]}" != "${BASH_REMATCH[2]}" ]]; then
					CHANGED=1
				fi
				printf '%s=%s\n' "$key" "${NEW[$key]}"
				continue
			fi
		fi
		printf '%s\n' "$line"
	done < "$target" > "$tmp"
	for key in "${APPEND_ORDER[@]}"; do
		if [[ -z "${SEEN[$key]+x}" ]] && [[ -n "${NEW[$key]+x}" ]]; then
			printf '%s=%s\n' "$key" "${NEW[$key]}" >> "$tmp"
			CHANGED=1
		fi
	done
}

collect_warnings() {
	if [[ "${CURRENT[DATABRICKS_HOST]:-}" == *"<"* ]]; then
		WARN+=("DATABRICKS_HOST still points at <workspace>; set the real warehouse host")
	fi
	if [[ "${CURRENT[DATABRICKS_TOKEN]:-}" == dapi_change_me ]]; then
		WARN+=("DATABRICKS_TOKEN is still a placeholder; extraction from Databricks needs a real token")
	fi
}

print_summary() {
	local target="$1"
	if (( CREATED )); then
		echo "setup_env: created $target with generated secrets"
	elif (( CHANGED )); then
		echo "setup_env: updated $target (backup at $BACKUP_PATH)"
	else
		echo "setup_env: $target already up to date"
	fi
	if (( ${#ROTATED[@]} )); then
		echo "setup_env: set ${ROTATED[*]}"
	fi
	local item
	for item in "${WARN[@]}"; do
		echo "setup_env: warning: $item" >&2
	done
}

cleanup() {
	[[ -n "${TMP_FILE:-}" ]] || return 0
	rm -f "$TMP_FILE"
}

main() {
	while [[ $# -gt 0 ]]; do
		case "$1" in
			--force) FORCE=1 ;;
			-h | --help) usage; return 0 ;;
			*) usage >&2; return 1 ;;
		esac
		shift
	done
	require_cmd openssl
	local repo template target
	repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
	template="$repo/$TEMPLATE_NAME"
	target="$repo/$TARGET_NAME"
	if [[ ! -f "$template" ]]; then
		echo "setup_env: template not found: $template" >&2
		return 1
	fi
	if [[ ! -f "$target" ]]; then
		cp "$template" "$target"
		CREATED=1
		CHANGED=1
	fi
	load_current "$target"
	resolve_managed
	resolve_alchemy
	resolve_gmail
	collect_warnings
	TMP_FILE="$(mktemp "$repo/.env.tmp.XXXXXX")"
	trap cleanup EXIT
	rewrite_target "$target" "$TMP_FILE"
	if (( CHANGED )); then
		if (( ! CREATED )); then
			BACKUP_PATH="${target}.bak.$(date +%Y%m%d%H%M%S)"
			cp "$target" "$BACKUP_PATH"
		fi
		mv "$TMP_FILE" "$target"
		chmod 600 "$target" 2>/dev/null || true
	fi
	print_summary "$target"
}

main "$@"
