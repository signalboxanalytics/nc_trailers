{% macro delete_duplicates(table_name) %}

{% set temp_table_name = 'temp_technician_performance_update' %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
SELECT _FILE, _LINE, _MODIFIED, TYPE, TECHNICIAN, HRS_CLOCKED_ON, PRODUCTIVITY, CLOCKED_HRS_INVOICED, CLOCKED_HRS_WIP, INVOICE_HRS, INVOICE_HRS_WIP, EFFICIENCY, PROFICIENCY, OTHER_TASKS_HRS,
       COALESCE(TRY_TO_DATE(DATE, 'MM/DD/YYYY'), TRY_TO_DATE(DATE, 'M/D/YYYY'), TRY_TO_DATE(DATE, 'MM-DD-YYYY'), TRY_TO_DATE(DATE, 'M-D-YYYY'), TRY_TO_DATE(DATE, 'YYYY-MM-DD')) as date_formatted, 
       _FIVETRAN_SYNCED, UNINVOICED_COST, INVOICED_COST, HOURS_WORKED, ROW_NUMBER() OVER (PARTITION BY date_formatted, TECHNICIAN ORDER BY _MODIFIED DESC) AS row_num
FROM {{ source('blackpurl_production', table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source('blackpurl_production', table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source('blackpurl_production', table_name) }} (_FILE, _LINE, _MODIFIED, TYPE, TECHNICIAN, HRS_CLOCKED_ON, PRODUCTIVITY, CLOCKED_HRS_INVOICED, CLOCKED_HRS_WIP, INVOICE_HRS, INVOICE_HRS_WIP, EFFICIENCY, PROFICIENCY, OTHER_TASKS_HRS, DATE, _FIVETRAN_SYNCED, UNINVOICED_COST, INVOICED_COST, HOURS_WORKED)
SELECT _FILE, _LINE, _MODIFIED, TYPE, TECHNICIAN, HRS_CLOCKED_ON, PRODUCTIVITY, CLOCKED_HRS_INVOICED, CLOCKED_HRS_WIP, INVOICE_HRS, INVOICE_HRS_WIP, EFFICIENCY, PROFICIENCY, OTHER_TASKS_HRS, date_formatted, _FIVETRAN_SYNCED, UNINVOICED_COST, INVOICED_COST, HOURS_WORKED
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}
