CREATE SCHEMA IF NOT EXISTS capstone;
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'putmeup';
CREATE ROLE checker WITH LOGIN PASSWORD 'putmeup';

CREATE TABLE IF NOT EXISTS capstone.orders (
    dataset_id text NOT NULL,
    id text NOT NULL,
    campaign_id text NOT NULL,
    status text NOT NULL,
    amount_cents bigint NOT NULL,
    version integer NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (dataset_id, id)
);
CREATE TABLE IF NOT EXISTS capstone.order_items (
    dataset_id text NOT NULL,
    id text NOT NULL,
    order_id text NOT NULL,
    sku text NOT NULL,
    quantity integer NOT NULL,
    unit_price_cents bigint NOT NULL,
    PRIMARY KEY (dataset_id, id),
    FOREIGN KEY (dataset_id, order_id)
        REFERENCES capstone.orders (dataset_id, id)
);
CREATE TABLE IF NOT EXISTS capstone.payments (
    dataset_id text NOT NULL,
    id text NOT NULL,
    order_id text NOT NULL,
    kind text NOT NULL,
    amount_cents bigint NOT NULL,
    occurred_at timestamptz NOT NULL,
    PRIMARY KEY (dataset_id, id),
    FOREIGN KEY (dataset_id, order_id)
        REFERENCES capstone.orders (dataset_id, id)
);

ALTER TABLE capstone.orders REPLICA IDENTITY FULL;
ALTER TABLE capstone.order_items REPLICA IDENTITY FULL;
ALTER TABLE capstone.payments REPLICA IDENTITY FULL;

CREATE PUBLICATION capstone_pub FOR TABLE capstone.orders, capstone.order_items, capstone.payments;

GRANT USAGE ON SCHEMA capstone TO debezium, checker;
GRANT SELECT ON capstone.orders, capstone.order_items, capstone.payments TO debezium, checker;
GRANT INSERT, UPDATE, DELETE ON capstone.orders, capstone.order_items, capstone.payments TO checker;
