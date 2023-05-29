{% macro delete_invalid_rows(table_name) %}

DELETE FROM {{ source('blackpurl_production', table_name) }} WHERE DATE is null and TECHNICIAN is null;

{% endmacro %}
