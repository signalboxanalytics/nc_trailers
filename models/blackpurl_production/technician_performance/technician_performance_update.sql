-- technician_performance_update.sql
{{ config(
	materialized='incremental',
  pre_hook="DELETE FROM {{ source('blackpurl_production', 'technician_performance_update') }};",
) }}


WITH stage_prep AS (
SELECT * FROM {{ source('blackpurl_production', 'technician_performance_stage') }} as s
WHERE s.TECHNICIAN is not null),

prod_prep AS (
SELECT * FROM {{ source('blackpurl_production', 'technician_performance_update') }} as p
WHERE p.TECHNICIAN is not null), 

incremental_data AS (
  SELECT
    s.*
  FROM stage_prep s
  LEFT JOIN prod_prep t ON s.TECHNICIAN = t.TECHNICIAN AND s.DATE = t.DATE
  WHERE t.TECHNICIAN IS NULL)

{% if is_incremental() %}
  -- Incremental logic
  SELECT * FROM incremental_data
{% else %}
  -- Full-refresh logic
  SELECT * FROM stage_prep
{% endif %}


-- WHERE EXISTS (
--     SELECT 1
--     FROM NC_TRAILERS.BLACKPURL_PRODUCTION.TECHNICIAN_PERFORMANCE_UPDATE sub
--     WHERE COALESCE(TRY_TO_DATE(main.DATE, 'MM/DD/YYYY'), TRY_TO_DATE(main.DATE, 'M/D/YYYY'), TRY_TO_DATE(main.DATE, 'MM-DD-YYYY'), TRY_TO_DATE(main.DATE, 'M-D-YYYY')) = 
--           COALESCE(TRY_TO_DATE(sub.DATE, 'MM/DD/YYYY'), TRY_TO_DATE(sub.DATE, 'M/D/YYYY'), TRY_TO_DATE(sub.DATE, 'MM-DD-YYYY'), TRY_TO_DATE(sub.DATE, 'M-D-YYYY'))
--       AND main.TECHNICIAN = sub.TECHNICIAN
--       AND main._MODIFIED < sub._MODIFIED
--     )