"""preps postgresql tables or kafka topics without generating data."""

import argparse
import asyncio
import os
import re
from pathlib import Path

import psycopg
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError, for_code


async def create_schema(dsn):
    async with await psycopg.AsyncConnection.connect(dsn, connect_timeout=10) as conn:
        await conn.execute(
            Path(__file__).with_name("shop_tables_ddl.sql").read_text(encoding="utf-8")
        )
    print("Схема capstone и три таблицы готовы. Данные не менял, CDC не настраивал.")


async def create_topics(brokers, prefix, partitions):
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers, request_timeout_ms=15000)
    try:
        await admin.start()
        topics = [
            NewTopic(
                f"{prefix}.{table}",
                num_partitions=partitions,
                replication_factor=1,
            )
            for table in ("orders", "order_items", "payments")
        ]
        response = await admin.create_topics(topics)
        for topic, code, message in response.topic_errors:
            match code:
                case 0:
                    print(f"{topic}: создан, partitions={partitions}")
                case TopicAlreadyExistsError.errno:
                    print(f"{topic}: уже есть, оставляю как есть")
                case _:
                    raise for_code(code)(f"{topic}: {message}")
    finally:
        await admin.close()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Подготовить место для данных. Самих данных пока не будет."
    )
    modes = parser.add_subparsers(dest="target", required=True)
    postgres = modes.add_parser("postgres", help="Создать схему capstone и три таблицы")
    postgres.add_argument(
        "--dsn", help="Подключение к PostgreSQL; можно через POSTGRES_DSN"
    )
    kafka = modes.add_parser(
        "kafka", help="Создать три топика для одного Kafka-брокера"
    )
    kafka.add_argument("--brokers", default="localhost:19092", help="Адреса Kafka")
    kafka.add_argument(
        "--topic-prefix", default="capstone", help="Префикс имён топиков"
    )
    kafka.add_argument(
        "--partitions",
        type=int,
        default=2,
        help="Количество партиций в каждом новом топике",
    )
    args = parser.parse_args(argv)
    match args.target:
        case "postgres":
            args.dsn = args.dsn or os.environ.get("POSTGRES_DSN")
            if not args.dsn:
                parser.error(
                    "Задайте POSTGRES_DSN или --dsn с подключением к своей учебной базе"
                )
        case "kafka":
            if args.partitions <= 0:
                parser.error("Число partitions должно быть больше 0")
            if not re.fullmatch(r"[a-zA-Z0-9_-]{1,200}", args.topic_prefix):
                parser.error("--topic-prefix: 1–200 символов a-z, A-Z, 0-9, _ и -")
    return args


async def main():
    args = parse_args()
    match args.target:
        case "postgres":
            await create_schema(args.dsn)
        case "kafka":
            await create_topics(
                args.brokers,
                args.topic_prefix,
                args.partitions,
            )


if __name__ == "__main__":
    asyncio.run(main())
