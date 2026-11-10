_:
    @just --list

# just run kafka --orders 1000 --rate 500
run +args:
    uv run --locked generate.py {{args}}

# shows a small dataset and exists (no i/o).
preview:
    uv run --locked generate.py kafka --orders 3 --dataset example --dry-run

help:
    uv run --locked generate.py --help

# test:
#     uv run --locked python -m unittest discover -s . -v

# create the source tables in postgres
create-pg-schema:
    uv run --locked create_stuff.py postgres

# create three kafka topics
create-topics brokers="localhost:19092" prefix="capstone" partitions="2":
    uv run --locked create_stuff.py kafka \
        --brokers {{quote(brokers)}} \
        --topic-prefix {{quote(prefix)}} \
        --partitions {{quote(partitions)}}

# create clickhouse database "capstone"
click_create_db:
    docker compose exec -T clickhouse clickhouse-client --user cocojumbo --password putmeup --query 'CREATE DATABASE IF NOT EXISTS capstone'
