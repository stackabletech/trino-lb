First start a postgres:

```bash
docker run --rm -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=admin postgres
```

Afterwards you set the `DATABASE_URL` env var and prepare stuff for offline compilation:

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost/postgres

cd trino-lb-persistence

cargo sqlx migrate run --source src/postgres/migrations
cargo sqlx prepare --workspace
```
