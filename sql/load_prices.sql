-- 1. Временная таблица: staging без дублей
DROP TABLE IF EXISTS tmp_price_staging;

CREATE TEMP TABLE tmp_price_staging AS
WITH raw AS (
  SELECT
    id AS price_id,
    productid AS product_id,
    pricecomponentid,
    pricecomponent,
    price,
    unit,
    valid_from::date,
    valid_until::date,
    modificationdate::date,
    source_table
  FROM staging.prices_staging_view
),

-- Удаляем полные дубли, оставляя только MAX(source_table)
deduplicated AS (
  SELECT DISTINCT ON (price_id, modificationdate)
    price_id,
    product_id,
    pricecomponentid,
    pricecomponent,
    price,
    unit,
    valid_from,
    valid_until,
    modificationdate,
    MAX(source_table) OVER (PARTITION BY price_id, modificationdate) AS source_table
  FROM raw
),

prepared AS (
  SELECT *,
    TO_TIMESTAMP(
      REGEXP_REPLACE(source_table, '^.*_(\d{8})_(\d{6})$', '\1\2'),
      'YYYYMMDDHH24MISS'
    ) AS extracted_at
  FROM deduplicated
)

-- Вставляем только новые строки, которых ещё нет
SELECT *
FROM prepared
WHERE NOT EXISTS (
  SELECT 1
  FROM dwh.prices p
  WHERE p.price_id = prepared.price_id
    AND p.modificationdate = prepared.modificationdate
);

-- 2. Вставка новых записей
INSERT INTO dwh.prices (
  price_id,
  product_id,
  pricecomponentid,
  pricecomponent,
  price,
  unit,
  valid_from,
  valid_until,
  modificationdate,
  extracted_at,
  source_table
)
SELECT
  price_id,
  product_id,
  pricecomponentid,
  pricecomponent,
  price,
  unit,
  valid_from,
  valid_until,
  modificationdate,
  extracted_at,
  source_table
FROM tmp_price_staging;
  
-- 3. Сброс всех флагов is_current (для тех, у кого появилась более свежая версия)
UPDATE dwh.prices p
SET is_current = FALSE
WHERE EXISTS (
  SELECT 1
  FROM dwh.prices newer
  WHERE newer.price_id = p.price_id
    AND newer.modificationdate > p.modificationdate
);

-- 4. Помечаем самую свежую запись по каждому price_id как текущую
UPDATE dwh.prices p
SET is_current = TRUE
WHERE NOT EXISTS (
  SELECT 1
  FROM dwh.prices newer
  WHERE newer.price_id = p.price_id
    AND newer.modificationdate > p.modificationdate
);
