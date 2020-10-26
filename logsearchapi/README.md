# Log Search API Server for MinIO

## Development setup

1. Start Postgresql server in container:

```shell
docker run --rm -it -e "POSTGRES_PASSWORD=example" -p 5432:5432 postgres:13-alpine -c "log_statement=all"
```

2. Start logsearchapi server:

```shell
go build
PG_CONN_STR="postgres://postgres:example@localhost/postgres" AUDIT_AUTH_TOKEN=xxx RETENTION_MONTHS=3 ./logsearchapi

```

3. Minio setup:

```shell
mc admin config set myminio audit_webhook:1 'endpoint=http://localhost:8080/api/ingest?token=xxx'

mc admin service restart myminio
```
