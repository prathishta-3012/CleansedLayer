{% macro clean_timestamp(col_name) %}
    TO_TIMESTAMP({{ col_name }})
{% endmacro %}


-- Raw Value	Output	Notes
-- '2024-04-02 13:45:00'	2024-04-02 13:45:00	Full timestamp preserved
-- '2024-04-02T13:45:00Z'	2024-04-02 13:45:00	Parsed ISO timestamp
-- NULL	NULL	Preserved