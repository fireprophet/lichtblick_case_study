-- Подготовка расчета периодов цен
DROP TABLE IF EXISTS dwh.price_periods;

CREATE TABLE dwh.price_periods (
  snapshot_date DATE NOT NULL,               -- дата слепка (макс. extracted_at на момент расчёта)
  product_id INT NOT NULL,
  base_pricecomponentid INT,
  working_pricecomponentid INT,
  base_id BIGINT,
  working_id BIGINT,
  valid_from DATE NOT NULL,
  valid_until DATE NOT NULL,
  baseprice NUMERIC,
  workingprice NUMERIC,
  inserted_at TIMESTAMP DEFAULT now(),

  -- уникальность периодов на дату слепка
  CONSTRAINT uq_price_period UNIQUE (snapshot_date, product_id, valid_from, valid_until)
);


-- 0. Максимальная дата extracted_at
WITH max_extract AS (
  SELECT MAX(extracted_at::date) AS max_date FROM dwh.prices
),

-- 1. Отбор актуальных цен
prices_filtered AS (
  SELECT DISTINCT ON (price_id)
    price_id,
    product_id,
    pricecomponentid,
    pricecomponent,
    price,
    unit,
    valid_from,
    valid_until
  FROM dwh.prices
  WHERE pricecomponentid IN (1, 2)
  ORDER BY price_id, valid_until DESC
),

-- 2. Подготовка base_input
base_input AS (
  SELECT
    f.product_id,
    f.pricecomponentid,
    f.price_id,
    f.price,
    GREATEST(f.valid_from, date_trunc('year', f.valid_from)) AS valid_from,
    LEAST(f.valid_until, m.max_date) AS valid_until
  FROM prices_filtered f
  CROSS JOIN max_extract m
  WHERE f.pricecomponentid = 1
),

-- 3. Подготовка working_input
working_input AS (
  SELECT
    f.product_id,
    f.pricecomponentid,
    f.price_id,
    f.price,
    GREATEST(f.valid_from, date_trunc('year', f.valid_from)) AS valid_from,
    LEAST(f.valid_until, m.max_date) AS valid_until
  FROM prices_filtered f
  CROSS JOIN max_extract m
  WHERE f.pricecomponentid = 2
),

-- 4. Разворачивание периодов
base_periods AS (
  SELECT
    product_id,
    pricecomponentid,
    price_id,
    price,
    gs::date AS period_start,
    LEAST(valid_until, date_trunc('year', gs) + INTERVAL '1 year' - INTERVAL '1 day') AS period_end
  FROM base_input,
  generate_series(valid_from, valid_until, interval '1 year') AS gs
),

working_periods AS (
  SELECT
    product_id,
    pricecomponentid,
    price_id,
    price,
    gs::date AS period_start,
    LEAST(valid_until, date_trunc('year', gs) + INTERVAL '1 year' - INTERVAL '1 day') AS period_end
  FROM working_input,
  generate_series(valid_from, valid_until, interval '1 year') AS gs
),

-- 5. Объединение уникальных периодов
master_periods AS (
  SELECT DISTINCT
    product_id,
    period_start AS valid_from,
    period_end AS valid_until
  FROM base_periods
  UNION
  SELECT DISTINCT
    product_id,
    period_start,
    period_end
  FROM working_periods
),

-- 6. Финальный join
joined_periods AS (
  SELECT
    mp.product_id,
    bp.pricecomponentid AS base_pricecomponentid,
    wp.pricecomponentid AS working_pricecomponentid,
    bp.price_id AS base_id,
    wp.price_id AS working_id,
    mp.valid_from,
    mp.valid_until,
    bp.price AS baseprice,
    wp.price AS workingprice
  FROM master_periods mp
  LEFT JOIN base_periods bp
    ON mp.product_id = bp.product_id
   AND mp.valid_from = bp.period_start
   AND mp.valid_until = bp.period_end
  LEFT JOIN working_periods wp
    ON mp.product_id = wp.product_id
   AND mp.valid_from = wp.period_start
   AND mp.valid_until = wp.period_end
)

-- 7. Вставка в перманентную таблицу
INSERT INTO dwh.price_periods (
  snapshot_date,
  product_id,
  base_pricecomponentid,
  working_pricecomponentid,
  base_id,
  working_id,
  valid_from,
  valid_until,
  baseprice,
  workingprice
)
SELECT
  (SELECT max_date FROM max_extract),
  product_id,
  base_pricecomponentid,
  working_pricecomponentid,
  base_id,
  working_id,
  valid_from,
  valid_until,
  baseprice,
  workingprice
FROM joined_periods
ON CONFLICT (snapshot_date, product_id, valid_from, valid_until)
DO NOTHING;
