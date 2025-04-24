{% macro clean_url(col_name) %}
    CASE
        WHEN LOWER({{ col_name }}) LIKE 'http%' THEN TRIM({{ col_name }})
        ELSE NULL
    END
{% endmacro %}
