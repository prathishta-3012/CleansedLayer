{% macro clean_code(col_name) %}
    UPPER(
        REPLACE(
            TRIM({{ col_name }}),
            ' ',
            ''
        )
    )
{% endmacro %}


-- Input	Output
-- ' prd-123 '	'PRD-123'
-- 'item_ 99'	'ITEM_99'