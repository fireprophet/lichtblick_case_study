
DROP TABLE IF EXISTS dwh.contract_price_enriched;

CREATE TABLE dwh.contract_price_enriched (
  contract_id BIGINT NOT NULL,                    -- ID контракта
  productid INT NOT NULL,                         -- ID продукта
  period_start DATE NOT NULL,                     -- начало периода
  period_end DATE NOT NULL,                       -- конец периода
  status TEXT,                                    -- статус контракта
  consumption NUMERIC,                            -- годовое потребление
  startdate DATE NOT NULL,                        -- дата начала контракта
  enddate DATE,                                   -- дата окончания контракта (может быть NULL)
  createdat DATE NOT NULL,                        -- дата создания контракта
  baseprice NUMERIC,                              -- базовая цена
  workingprice NUMERIC,                           -- переменная цена
  days_in_period NUMERIC,                         -- дней в периоде
  days_since_start NUMERIC,                       -- дней с начала контракта
  contract_year INT,                              -- номер контрактного года
  total_contract_years INT,                       -- всего контрактных лет
  final_end DATE NOT NULL,                        -- фактический конец (enddate или текущая дата)
  terminated_mid_year BOOLEAN,                    -- флаг преждевременного завершения
  total_days_in_year NUMERIC,                     -- дней в контрактном году
  period_share NUMERIC(12,6),                     -- доля периода от года
  base_cost NUMERIC(18,6),                        -- фиксированная стоимость
  variable_cost NUMERIC(18,6),                    -- переменная стоимость
  weighted_consumption NUMERIC(18,6),             -- взвешенное потребление
  consumption_share NUMERIC(12,6),                -- доля потребления
  revenue NUMERIC(18,6),                          -- общая выручка
  baseprice_total_due NUMERIC(18,2),              -- сумма базовых платежей
  inserted_at TIMESTAMP DEFAULT now(),            -- дата вставки записи

  -- уникальность по контракту, продукту и началу периода
  CONSTRAINT uq_contract_price_enriched UNIQUE (contract_id, productid, period_start)
);

-- Удаление временных таблиц
DROP TABLE IF EXISTS contract_input;
DROP TABLE IF EXISTS contract_breakpoints;
DROP TABLE IF EXISTS contract_periods;
DROP TABLE IF EXISTS joined_periods_dedup;
DROP TABLE IF EXISTS contract_price_periods;

-- 1. Контракты с createdat
CREATE TEMP TABLE contract_input AS
SELECT DISTINCT
  contract_id,
  product_id AS productid,
  status,
  COALESCE(usagenet, usage) AS consumption,
  valid_from AS startdate,
  CASE 
    WHEN valid_until = DATE '9999-12-31' 
    THEN MAX(snapshot_date) OVER (PARTITION BY contract_id, product_id)
    ELSE valid_until 
  END AS enddate,
  createdat::date AS createdat
FROM dwh.contracts
WHERE status <> 'indelivery';

-- 2. Контрольные даты
CREATE TEMP TABLE contract_breakpoints AS
SELECT DISTINCT
  c.contract_id,
  c.productid,
  date_value
FROM contract_input c,
LATERAL (
  SELECT date_value FROM (
    SELECT c.startdate AS date_value
    UNION
    SELECT c.createdat AS date_value
    UNION
    SELECT c.enddate AS date_value
    UNION
    SELECT valid_from FROM dwh.price_periods
      WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dwh.price_periods)
        AND product_id = c.productid
    UNION
    SELECT valid_until FROM dwh.price_periods
      WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dwh.price_periods)
        AND product_id = c.productid
  ) d
  WHERE date_value IS NOT NULL
) AS bp;

-- 3. Разбиение на периоды
CREATE TEMP TABLE contract_periods AS
SELECT
  contract_id,
  productid,
  date_value AS period_start,
  LEAD(date_value) OVER (PARTITION BY contract_id ORDER BY date_value) AS period_end
FROM contract_breakpoints
WHERE date_value IS NOT NULL;

-- 4. Удаление нулевых интервалов
DELETE FROM contract_periods
WHERE period_end IS NULL OR period_start = period_end;

-- 5. Дедупликация цен
CREATE TEMP TABLE joined_periods_dedup AS
SELECT
  product_id,
  valid_from,
  valid_until,
  MAX(baseprice) AS baseprice,
  MAX(workingprice) AS workingprice
FROM dwh.price_periods
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM dwh.price_periods)
GROUP BY product_id, valid_from, valid_until;

-- 6. Присоединяем параметры и цены
CREATE TEMP TABLE contract_price_periods AS
SELECT
  p.contract_id,
  p.productid,
  p.period_start,
  p.period_end,
  ci.status,
  ci.consumption,
  ci.startdate,
  ci.enddate,
  ci.createdat,
  jp.baseprice,
  jp.workingprice
FROM contract_periods p
JOIN (
  SELECT DISTINCT ON (contract_id)
    contract_id,
    productid,
    status,
    consumption,
    startdate,
    enddate,
    createdat
  FROM contract_input
  ORDER BY contract_id, createdat DESC
) ci ON p.contract_id = ci.contract_id
LEFT JOIN joined_periods_dedup jp
  ON p.productid = jp.product_id
 AND p.period_start BETWEEN jp.valid_from AND jp.valid_until
WHERE
  p.period_start >= ci.startdate
  AND (ci.enddate IS NULL OR p.period_start < ci.enddate)
  AND p.period_start < CURRENT_DATE;

-- 7. Финальный расчёт и вставка
INSERT INTO dwh.contract_price_enriched (
  contract_id,
  productid,
  period_start,
  period_end,
  status,
  consumption,
  startdate,
  enddate,
  createdat,
  baseprice,
  workingprice,
  days_in_period,
  days_since_start,
  contract_year,
  total_contract_years,
  final_end,
  terminated_mid_year,
  total_days_in_year,
  period_share,
  base_cost,
  variable_cost,
  weighted_consumption,
  consumption_share,
  revenue,
  baseprice_total_due
)
WITH base_data AS (
  SELECT
    *,
    (period_end - period_start)::numeric AS days_in_period,
    (period_start - startdate)::numeric AS days_since_start,
    FLOOR((period_start - startdate)::numeric / 365.0) AS contract_year
  FROM contract_price_periods
),
years_per_contract AS (
  SELECT
    contract_id,
    startdate,
    enddate,
    CEIL((COALESCE(enddate, CURRENT_DATE) - startdate)::numeric / 365.0) AS total_contract_years,
    COALESCE(enddate, CURRENT_DATE) AS final_end
  FROM contract_price_periods
  GROUP BY contract_id, startdate, enddate
),
joined AS (
  SELECT
    b.*,
    y.total_contract_years,
    y.final_end,
    CASE 
      WHEN y.final_end < (b.startdate + (y.total_contract_years || ' years')::interval) 
        THEN true ELSE false
    END AS terminated_mid_year
  FROM base_data b
  JOIN years_per_contract y
    ON b.contract_id = y.contract_id AND b.startdate = y.startdate
    AND (
         b.enddate = y.enddate
      OR (b.enddate IS NULL AND y.enddate IS NULL)
    )
),
year_base_totals AS (
  SELECT
    contract_id,
    contract_year,
    SUM(days_in_period)::numeric AS total_days_in_year,
    MAX(baseprice) AS baseprice
  FROM joined
  GROUP BY contract_id, contract_year
),
enriched AS (
  SELECT
    j.*,
    y.total_days_in_year,
    ROUND((j.days_in_period / NULLIF(y.total_days_in_year, 0))::numeric, 6) AS period_share,
    ROUND((y.baseprice * (j.days_in_period / NULLIF(y.total_days_in_year, 0)))::numeric, 6) AS base_cost,
    ROUND(((COALESCE(workingprice, 0) / 100.0) * (consumption * j.days_in_period / 365.0))::numeric, 6) AS variable_cost,
    ROUND((consumption * j.days_in_period / 365.0)::numeric, 6) AS weighted_consumption,
    ROUND(((consumption * j.days_in_period / 365.0) / NULLIF(consumption, 0))::numeric, 6) AS consumption_share
  FROM joined j
  JOIN year_base_totals y
    ON j.contract_id = y.contract_id AND j.contract_year = y.contract_year
)
SELECT
  contract_id,
  productid,
  period_start,
  period_end,
  status,
  consumption,
  startdate,
  enddate,
  createdat,
  baseprice,
  workingprice,
  days_in_period,
  days_since_start,
  contract_year,
  total_contract_years,
  final_end,
  terminated_mid_year,
  total_days_in_year,
  period_share,
  base_cost,
  variable_cost,
  weighted_consumption,
  consumption_share,
  ROUND((base_cost + variable_cost)::numeric, 6) AS revenue,
  ROUND(total_contract_years::numeric * baseprice::numeric, 2) AS baseprice_total_due
FROM enriched;
