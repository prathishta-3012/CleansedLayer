{% macro clean_email(col_name) %}
    LOWER(
        TRIM({{ col_name }})
    )
{% endmacro %}

--Input: ' JohN.Doe@Example.COM '
--Output: 'john.doe@example.com'