CREATE SCHEMA IF NOT EXISTS dwh;

DROP TABLE IF EXISTS dwh.products;
DROP TABLE IF EXISTS dwh.prices;
DROP TABLE IF EXISTS dwh.contracts;

-- Таблица продуктов
CREATE TABLE dwh.products (
  product_sk SERIAL PRIMARY KEY,
  product_id BIGINT NOT NULL,
  productcode TEXT,
  productname TEXT,
  energy TEXT,
  consumptiontype TEXT,
  deleted SMALLINT DEFAULT 0,
  releasedversion BIGINT,
  valid_from DATE NOT NULL,
  valid_until DATE DEFAULT '9999-12-31',
  is_current BOOLEAN DEFAULT TRUE,
  modificationdate DATE NOT NULL,
  extracted_at TIMESTAMP,
  source_table TEXT,
  hash_value TEXT,
  inserted_at TIMESTAMP DEFAULT now(),
  UNIQUE (product_id, valid_from)
);

-- Таблица цен
CREATE TABLE dwh.prices (
  price_sk SERIAL PRIMARY KEY,
  price_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  pricecomponentid INT NOT NULL,
  pricecomponent TEXT,
  price NUMERIC(38,10),
  unit TEXT,
  valid_from DATE NOT NULL,
  valid_until DATE NOT NULL,
  modificationdate DATE NOT NULL,
  is_current BOOLEAN DEFAULT TRUE,
  extracted_at TIMESTAMP,
  source_table TEXT,
  inserted_at TIMESTAMP DEFAULT now(),
  UNIQUE (price_id, modificationdate)
);

-- Таблица контрактов
CREATE TABLE dwh.contracts (
  contract_sk SERIAL PRIMARY KEY,
  contract_id BIGINT NOT NULL,
  product_id INT NOT NULL,
  type TEXT,
  energy TEXT,
  usage INT,
  usagenet INT,
  createdat DATE,
  startdate DATE NOT NULL,
  enddate DATE,
  filingdatecancellation DATE,
  cancellationreason TEXT,
  city TEXT,
  status TEXT NOT NULL,
  modificationdate DATE NOT NULL,
  snapshot_date DATE NOT NULL,
  source_table TEXT,
  is_current BOOLEAN DEFAULT TRUE,
  is_valid BOOLEAN DEFAULT TRUE,
  valid_from DATE NOT NULL,
  valid_until DATE NOT NULL DEFAULT DATE '9999-12-31',
  inserted_at TIMESTAMP DEFAULT now(),
  hash_value TEXT,
  UNIQUE (contract_id, modificationdate)
);
