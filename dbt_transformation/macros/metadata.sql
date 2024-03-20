-- macros/metadata.sql

{% macro get_timestamp() %}
    '{{ run_started_at }}'
{% endmacro %}
