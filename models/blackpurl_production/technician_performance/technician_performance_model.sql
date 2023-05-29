-- technician_performance_update.sql
{{ config(
	materialized='incremental',
  incremental_strategy='merge',
  unique_key=['DATE', 'TECHNICIAN'],
  pre_hook=[
        "{% do delete_duplicates('technician_performance_stage') %}",
        "{% do delete_duplicates('technician_performance_update') %}",
        "{% do delete_invalid_rows('technician_performance_stage') %}",
        "{% do delete_invalid_rows('technician_performance_update') %}",
    ],
  post_hook="TRUNCATE TABLE {{ source('blackpurl_production', 'technician_performance_stage') }};"

) }}


WITH stage AS (
    SELECT
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
        COALESCE(TRY_TO_DATE(DATE, 'MM/DD/YYYY'), TRY_TO_DATE(DATE, 'M/D/YYYY'), TRY_TO_DATE(DATE, 'MM-DD-YYYY'), TRY_TO_DATE(DATE, 'M-D-YYYY'), TRY_TO_DATE(DATE, 'YYYY-MM-DD')) AS DATE,
        _FIVETRAN_SYNCED
    FROM {{ source('blackpurl_production', 'technician_performance_stage') }}
    WHERE formatted_date is not null and TECHNICIAN is not null
)

SELECT
_FILE, _LINE, _MODIFIED, TYPE, TECHNICIAN, HOURS_WORKED, HRS_CLOCKED_ON, PRODUCTIVITY, 
CLOCKED_HRS_INVOICED, CLOCKED_HRS_WIP, INVOICE_HRS, INVOICE_HRS_WIP, EFFICIENCY, PROFICIENCY, 
OTHER_TASKS_HRS, INVOICED_COST, UNINVOICED_COST, DATE, _FIVETRAN_SYNCED 
FROM stage

-- prod AS (
--     SELECT
--         _FILE,
--         _LINE,
--         _MODIFIED,
--         TYPE,
--         TECHNICIAN,
--         HOURS_WORKED,
--         HRS_CLOCKED_ON,
--         PRODUCTIVITY,
--         CLOCKED_HRS_INVOICED,
--         CLOCKED_HRS_WIP,
--         INVOICE_HRS,
--         INVOICE_HRS_WIP,
--         EFFICIENCY,
--         PROFICIENCY,
--         OTHER_TASKS_HRS,
--         INVOICED_COST,
--         UNINVOICED_COST,
--         COALESCE(TRY_TO_DATE(DATE, 'MM/DD/YYYY'), TRY_TO_DATE(DATE, 'M/D/YYYY'), TRY_TO_DATE(DATE, 'MM-DD-YYYY'), TRY_TO_DATE(DATE, 'M-D-YYYY'), TRY_TO_DATE(DATE, 'YYYY-MM-DD')) AS formatted_date,
--         _FIVETRAN_SYNCED
--     FROM {{ source('blackpurl_production', 'technician_performance_update') }}
--     WHERE formatted_date is not null and TECHNICIAN is not null
-- ),

-- matched_records AS (
--     SELECT
--         stage.*
--     FROM stage
--     JOIN prod
--         ON stage.formatted_date = prod.formatted_date
--         AND stage.TECHNICIAN = prod.TECHNICIAN
-- ),

-- updates AS (
--     SELECT
--         matched_records._FILE,
--         matched_records._LINE,
--         matched_records._MODIFIED,
--         matched_records.TYPE,
--         matched_records.TECHNICIAN,
--         matched_records.HOURS_WORKED,
--         matched_records.HRS_CLOCKED_ON,
--         matched_records.PRODUCTIVITY,
--         matched_records.CLOCKED_HRS_INVOICED,
--         matched_records.CLOCKED_HRS_WIP,
--         matched_records.INVOICE_HRS,
--         matched_records.INVOICE_HRS_WIP,
--         matched_records.EFFICIENCY,
--         matched_records.PROFICIENCY,
--         matched_records.OTHER_TASKS_HRS,
--         matched_records.INVOICED_COST,
--         matched_records.UNINVOICED_COST,
--         matched_records.formatted_date as DATE,
--         matched_records._FIVETRAN_SYNCED
--     FROM matched_records),

-- non_matched_records AS (
--     SELECT
--         stage.*
--     FROM stage
--     LEFT JOIN prod
--         ON stage.formatted_date = prod.formatted_date
--         AND stage.TECHNICIAN = prod.TECHNICIAN
--     WHERE prod.formatted_date IS NULL AND prod.TECHNICIAN IS NULL
-- ),

-- inserts AS (
--     SELECT
--         non_matched_records._FILE,
--         non_matched_records._LINE,
--         non_matched_records._MODIFIED,
--         non_matched_records.TYPE,
--         non_matched_records.TECHNICIAN,
--         non_matched_records.HOURS_WORKED,
--         non_matched_records.HRS_CLOCKED_ON,
--         non_matched_records.PRODUCTIVITY,
--         non_matched_records.CLOCKED_HRS_INVOICED,
--         non_matched_records.CLOCKED_HRS_WIP,
--         non_matched_records.INVOICE_HRS,
--         non_matched_records.INVOICE_HRS_WIP,
--         non_matched_records.EFFICIENCY,
--         non_matched_records.PROFICIENCY,
--         non_matched_records.OTHER_TASKS_HRS,
--         non_matched_records.INVOICED_COST,
--         non_matched_records.UNINVOICED_COST,
--         non_matched_records.formatted_date as DATE,
--         non_matched_records._FIVETRAN_SYNCED
--     FROM non_matched_records)



-- SELECT
-- _FILE, _LINE, _MODIFIED, TYPE, TECHNICIAN, HOURS_WORKED, HRS_CLOCKED_ON, PRODUCTIVITY, 
-- CLOCKED_HRS_INVOICED, CLOCKED_HRS_WIP, INVOICE_HRS, INVOICE_HRS_WIP, EFFICIENCY, PROFICIENCY, 
-- OTHER_TASKS_HRS, INVOICED_COST, UNINVOICED_COST, DATE, _FIVETRAN_SYNCED 
-- FROM updates

-- UNION

-- SELECT
-- _FILE, _LINE, _MODIFIED, TYPE, TECHNICIAN, HOURS_WORKED, HRS_CLOCKED_ON, PRODUCTIVITY, 
-- CLOCKED_HRS_INVOICED, CLOCKED_HRS_WIP, INVOICE_HRS, INVOICE_HRS_WIP, EFFICIENCY, PROFICIENCY, 
-- OTHER_TASKS_HRS, INVOICED_COST, UNINVOICED_COST, DATE, _FIVETRAN_SYNCED 
-- FROM inserts
