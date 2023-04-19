-- technician_performance_update.sql
{{ config(
	materialized='incremental',
  pre_hook=["DELETE FROM {{ source('blackpurl_production', 'technician_performance_stage') }} main
              WHERE EXISTS (
                SELECT 1
                FROM {{ source('blackpurl_production', 'technician_performance_stage') }} sub
                WHERE COALESCE(TRY_TO_DATE(main.DATE, 'MM/DD/YYYY'), TRY_TO_DATE(main.DATE, 'M/D/YYYY'), TRY_TO_DATE(main.DATE, 'MM-DD-YYYY'), TRY_TO_DATE(main.DATE, 'M-D-YYYY')) = 
                      COALESCE(TRY_TO_DATE(sub.DATE, 'MM/DD/YYYY'), TRY_TO_DATE(sub.DATE, 'M/D/YYYY'), TRY_TO_DATE(sub.DATE, 'MM-DD-YYYY'), TRY_TO_DATE(sub.DATE, 'M-D-YYYY'))
                  AND main.TECHNICIAN = sub.TECHNICIAN
                  AND main._MODIFIED < sub._MODIFIED
                );",

            "DELETE FROM {{ source('blackpurl_production', 'technician_performance_update') }} main
              WHERE EXISTS (
              SELECT 1
              FROM {{ source('blackpurl_production', 'technician_performance_stage') }} sub
              WHERE COALESCE(TRY_TO_DATE(main.DATE, 'MM/DD/YYYY'), TRY_TO_DATE(main.DATE, 'M/D/YYYY'), TRY_TO_DATE(main.DATE, 'MM-DD-YYYY'), TRY_TO_DATE(main.DATE, 'M-D-YYYY')) = 
                    COALESCE(TRY_TO_DATE(sub.DATE, 'MM/DD/YYYY'), TRY_TO_DATE(sub.DATE, 'M/D/YYYY'), TRY_TO_DATE(sub.DATE, 'MM-DD-YYYY'), TRY_TO_DATE(sub.DATE, 'M-D-YYYY'))
              AND main.TECHNICIAN = sub.TECHNICIAN
              );"],
  post_hook="TRUNCATE TABLE {{ source('blackpurl_production', 'technician_performance_stage') }};"

) }}

-- Merge stage table into production table
    MERGE INTO {{ source('blackpurl_production', 'technician_performance_update') }} prod
    USING {{ source('blackpurl_production', 'technician_performance_stage') }} stage
    ON  COALESCE(TRY_TO_DATE(prod.DATE, 'MM/DD/YYYY'), TRY_TO_DATE(prod.DATE, 'M/D/YYYY'), TRY_TO_DATE(prod.DATE, 'MM-DD-YYYY'), TRY_TO_DATE(prod.DATE, 'M-D-YYYY')) = 
          COALESCE(TRY_TO_DATE(stage.DATE, 'MM/DD/YYYY'), TRY_TO_DATE(stage.DATE, 'M/D/YYYY'), TRY_TO_DATE(stage.DATE, 'MM-DD-YYYY'), TRY_TO_DATE(stage.DATE, 'M-D-YYYY'))
    AND prod.TECHNICIAN = stage.TECHNICIAN
    WHEN MATCHED THEN UPDATE
    SET
        prod._FILE = stage._FILE,
        prod._LINE = stage._LINE,
        prod._MODIFIED = stage._MODIFIED,
        prod.TYPE = stage.TYPE,
        prod.TECHNICIAN = stage.TECHNICIAN,
        prod.HOURS_WORKED = stage.HOURS_WORKED,
        prod.HRS_CLOCKED_ON = stage.HRS_CLOCKED_ON,
        prod.PRODUCTIVITY = stage.PRODUCTIVITY,
        prod.CLOCKED_HRS_INVOICED = stage.CLOCKED_HRS_INVOICED,
        prod.CLOCKED_HRS_WIP = stage.CLOCKED_HRS_WIP,
        prod.INVOICE_HRS = stage.INVOICE_HRS,
        prod.INVOICE_HRS_WIP = stage.INVOICE_HRS_WIP,
        prod.EFFICIENCY = stage.EFFICIENCY,
        prod.PROFICIENCY = stage.PROFICIENCY,
        prod.OTHER_TASKS_HRS = stage.OTHER_TASKS_HRS,
        prod.INVOICED_COST = stage.INVOICED_COST,
        prod.UNINVOICED_COST = stage.UNINVOICED_COST,
        prod.DATE = COALESCE(TRY_TO_DATE(stage.DATE, 'MM/DD/YYYY'), TRY_TO_DATE(stage.DATE, 'M/D/YYYY'), TRY_TO_DATE(stage.DATE, 'MM-DD-YYYY'), TRY_TO_DATE(stage.DATE, 'M-D-YYYY')),
        prod._FIVETRAN_SYNCED = stage._FIVETRAN_SYNCED
    WHEN NOT MATCHED THEN INSERT
      (
        _FILE,
        _LINE,
        _MODIFIED,
        TYPE,
        TECHNICIAN,
        HOURS_WORKED,
        HRS_CLOCKED_ON,
        PRODUCTIVITY,
        CLOCKED_HRS_INVOICED,
        CLOCKED_HRS_WIP,
        INVOICE_HRS,
        INVOICE_HRS_WIP,
        EFFICIENCY,
        PROFICIENCY,
        OTHER_TASKS_HRS,
        INVOICED_COST,
        UNINVOICED_COST,
        DATE,
        _FIVETRAN_SYNCED
      )
    VALUES
    (
        stage._FILE,
        stage._LINE,
        stage._MODIFIED,
        stage.TYPE,
        stage.TECHNICIAN,
        stage.HOURS_WORKED,
        stage.HRS_CLOCKED_ON,
        stage.PRODUCTIVITY,
        stage.CLOCKED_HRS_INVOICED,
        stage.CLOCKED_HRS_WIP,
        stage.INVOICE_HRS,
        stage.INVOICE_HRS_WIP,
        stage.EFFICIENCY,
        stage.PROFICIENCY,
        stage.OTHER_TASKS_HRS,
        stage.INVOICED_COST,
        stage.UNINVOICED_COST,
        COALESCE(TRY_TO_DATE(stage.DATE, 'MM/DD/YYYY'), TRY_TO_DATE(stage.DATE, 'M/D/YYYY'), TRY_TO_DATE(stage.DATE, 'MM-DD-YYYY'), TRY_TO_DATE(stage.DATE, 'M-D-YYYY')),
        stage._FIVETRAN_SYNCED
    );

-----------

-- WITH stage_prep AS (
-- SELECT * FROM {{ source('blackpurl_production', 'technician_performance_stage') }} as s
-- WHERE s.TECHNICIAN is not null),

-- prod_prep AS (
-- SELECT * FROM {{ source('blackpurl_production', 'technician_performance_update') }} as p
-- WHERE p.TECHNICIAN is not null), 

-- incremental_data AS (
--   SELECT
--     s.*
--   FROM stage_prep s
--   LEFT JOIN prod_prep t ON s.TECHNICIAN = t.TECHNICIAN AND s.DATE = t.DATE
--   WHERE t.TECHNICIAN IS NULL)

-- {% if is_incremental() %}
--   -- Incremental logic
--   SELECT * FROM incremental_data
-- {% else %}
--   -- Full-refresh logic
--   SELECT * FROM stage_prep
-- {% endif %}


