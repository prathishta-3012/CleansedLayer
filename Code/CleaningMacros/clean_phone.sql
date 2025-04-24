{% macro clean_phone(col_name) %}
    RIGHT(
        REGEXP_REPLACE(
            TRIM({{ col_name }}),
            '[^0-9]',
            ''
        ),
        10
    )
{% endmacro %}


---Input: ' +91-99912 34567 ', 
--Output: '9991234567'