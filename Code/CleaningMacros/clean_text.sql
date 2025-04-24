{% macro clean_text(col_name) %}
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            TRIM({{ col_name }}),
            '(^|\\s)(\\w)',
            '\\1' || UPPER('\\2')
        ),
        '[!@#$%^*]',
        ''
    )
{% endmacro %}



--Input: ' acCeler@tor!! '
--Output: 'Accelerator'