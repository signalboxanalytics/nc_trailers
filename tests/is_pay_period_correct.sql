--The virtual assistants should always key in the first date of the pay period which will either be 1 or 16
SELECT
DAY(COALESCE(TRY_TO_DATE({{ column_name }}, 'MM/DD/YYYY'), TRY_TO_DATE({{ column_name }}, 'M/D/YYYY'), TRY_TO_DATE({{ column_name }}, 'MM-DD-YYYY'), TRY_TO_DATE({{ column_name }}, 'M-D-YYYY'), TRY_TO_DATE({{ column_name }}, 'YYYY-MM-DD'))) as DAY_NUMBER
FROM {{ ref('blackpurl_production', 'technician_performance_stage') }}
HAVING NOT (DAY_NUMBER = 1 or DAY_NUMBER = 16)