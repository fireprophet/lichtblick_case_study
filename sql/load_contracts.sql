-- 1. Временная таблица с фильтром и подготовкой данных
-- 1. Подготовка staging таблицы с фильтрацией и дедупликацией
DROP TABLE IF EXISTS tmp_contracts_stage;

CREATE TEMP TABLE tmp_contracts_stage AS
WITH staging AS (
  SELECT
    id AS contract_id,
    productid AS product_id,
    type,
    energy,
    usage,
    usagenet,
    createdat::date,
    startdate::date,
    enddate::date,
    filingdatecancellation::date,
    cancellationreason,
    city,
    status,
    modificationdate::date,
    TO_DATE(LEFT(source_table, 8), 'YYYYMMDD') AS snapshot_date,
    source_table
  FROM staging.contracts_staging_view
  WHERE COALESCE(usage, 0) <= 15000
    AND COALESCE(usagenet, 0) <= 15000
),
deduped AS (
  SELECT DISTINCT ON (contract_id, modificationdate)
    *,
    md5(CONCAT_WS('::',
      product_id, type, energy, usage, usagenet, createdat, startdate, enddate,
      filingdatecancellation, cancellationreason, city, status
    )) AS hash_value
  FROM staging
  ORDER BY contract_id, modificationdate, source_table DESC
)
SELECT * FROM deduped;

-- 2. Вставка новых версий (если modificationdate ещё не существует)
INSERT INTO dwh.contracts (
  contract_id,
  product_id,
  type,
  energy,
  usage,
  usagenet,
  createdat,
  startdate,
  enddate,
  filingdatecancellation,
  cancellationreason,
  city,
  status,
  modificationdate,
  snapshot_date,
  source_table,
  is_current,
  is_valid,
  valid_from,
  valid_until,
  inserted_at,
  hash_value
)
SELECT
  contract_id,
  product_id,
  type,
  energy,
  usage,
  usagenet,
  createdat,
  startdate,
  enddate,
  filingdatecancellation,
  cancellationreason,
  city,
  status,
  modificationdate,
  snapshot_date,
  source_table,
  TRUE,
  CASE WHEN status IN ('cancelled', 'terminated') THEN FALSE ELSE TRUE END,
  modificationdate,
  DATE '9999-12-31',
  NOW(),
  hash_value
FROM tmp_contracts_stage AS src
WHERE NOT EXISTS (
  SELECT 1
  FROM dwh.contracts AS tgt
  WHERE tgt.contract_id = src.contract_id
    AND tgt.modificationdate = src.modificationdate
);

-- 3. Закрытие предыдущих версий (всех кроме последней на contract_id)
WITH ranked_versions AS (
  SELECT
    contract_sk,
    contract_id,
    modificationdate,
    LEAD(modificationdate) OVER (PARTITION BY contract_id ORDER BY modificationdate) AS next_modificationdate,
    ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY modificationdate DESC) AS rn
  FROM dwh.contracts
  WHERE is_current = TRUE
)
UPDATE dwh.contracts AS tgt
SET is_current = FALSE,
    valid_until = rv.next_modificationdate - INTERVAL '1 day'
FROM ranked_versions rv
WHERE tgt.contract_sk = rv.contract_sk
  AND rv.rn > 1;
