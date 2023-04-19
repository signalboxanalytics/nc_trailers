-- technician_performance_update.sql
{{ config(
	materialized='incremental'
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
