-- macros/is_full_refresh.sql
{% macro is_full_refresh() %}
  {% if execute %}
    {{ return(flags.FULL_REFRESH) }}
  {% else %}
    {{ return('') }}
  {% endif %}
{% endmacro %}
