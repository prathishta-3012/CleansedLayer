{% macro clean_number(col_name, col_type='number') %}
    {% if col_type in ['text', 'varchar', 'string'] %}
        COALESCE(
            ROUND(
                ABS(
                    TRY_TO_NUMBER(NULLIF({{ col_name }}, ''))
                ),
                4
            ),
            0
        )
    {% else %}
        COALESCE(
            ROUND(
                ABS({{ col_name }}),
                4
            ),
            0
        )
    {% endif %}
{% endmacro %}



-- Input Value	    Output	    Explanation
-- NULL	            0	        Replaces null
-- -55.78	        56	        ABS → 55.78 → rounded to 56
-- 23.3	            23	        Rounded
-- -100.99	        101	        ABS and rounded