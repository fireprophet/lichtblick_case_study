-- 1. Подготовка: staging без дублей
DROP TABLE IF EXISTS tmp_changed;

-- 1. MERGE с дедупликацией дубликатов по всем полям (кроме source_table)

MERGE INTO dwh.products AS tgt
USING (
  -- staging с предварительной дедупликацией по бизнес-ключу
  WITH raw AS (
    SELECT
      id AS product_id,
      productcode,
      productname,
      energy,
      consumptiontype,
      deleted,
      CASE 
        WHEN releasedversion::text = 'no_data' THEN -1
        ELSE releasedversion::int
      END AS releasedversion,
      modificationdate::date AS modificationdate,
      source_table
    FROM staging.products_staging_view
  ),

  deduplicated_raw AS (
    -- сгруппировать по уникальному состоянию продукта
    SELECT
      product_id,
      productcode,
      productname,
      energy,
      consumptiontype,
      deleted,
      releasedversion,
      modificationdate,
      MAX(source_table) AS source_table
    FROM raw
    GROUP BY
      product_id,
      productcode,
      productname,
      energy,
      consumptiontype,
      deleted,
      releasedversion,
      modificationdate
  ),

  prepared AS (
    SELECT
      *,
      TO_TIMESTAMP(
        REGEXP_REPLACE(source_table, '^.*_(\d{8})_(\d{6})$', '\1\2'),
        'YYYYMMDDHH24MISS'
      ) AS extracted_at,
      md5(CONCAT_WS('::',
        productcode, productname, energy, consumptiontype, deleted, releasedversion
      )) AS hash_value
    FROM deduplicated_raw
  ),

  -- оставляем по 1 строке на product_id — последнюю по дате
  deduplicated AS (
    SELECT DISTINCT ON (product_id)
      *
    FROM prepared
    ORDER BY product_id, modificationdate DESC
  )

  SELECT * FROM deduplicated
) AS src

-- 2. Условие сопоставления: по текущей версии
ON tgt.product_id = src.product_id AND tgt.is_current = TRUE

-- 3. Если есть изменения — закрываем старую версию
WHEN MATCHED AND tgt.hash_value <> src.hash_value THEN
  UPDATE SET
    valid_until = src.modificationdate - INTERVAL '1 day',
    is_current = FALSE

-- 4. Если новой версии ещё нет — добавляем
WHEN NOT MATCHED
  AND NOT EXISTS (
    SELECT 1
    FROM dwh.products p
    WHERE p.product_id = src.product_id
      AND p.valid_from = src.modificationdate
  )
THEN INSERT (
  product_id,
  productcode,
  productname,
  energy,
  consumptiontype,
  deleted,
  releasedversion,
  valid_from,
  valid_until,
  is_current,
  modificationdate,
  extracted_at,
  source_table,
  hash_value,
  inserted_at
)
VALUES (
  src.product_id,
  src.productcode,
  src.productname,
  src.energy,
  src.consumptiontype,
  src.deleted,
  src.releasedversion,
  src.modificationdate,
  '9999-12-31',
  TRUE,
  src.modificationdate,
  src.extracted_at,
  src.source_table,
  src.hash_value,
  NOW()
);
