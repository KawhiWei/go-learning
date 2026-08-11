#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
DATABASE_URL=${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/nino?sslmode=disable}
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$ROOT/migrations/001_create_users.sql"
