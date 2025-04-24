{% macro clean_date_string(col_name) %}
    CASE 
        -- Likely DD-MM-YYYY (e.g., 13-04-2024)
        WHEN TRY_CAST(SPLIT_PART({{ col_name }}, '-', 1) AS INTEGER) > 12
          AND TRY_CAST(SPLIT_PART({{ col_name }}, '-', 2) AS INTEGER) <= 12
        THEN TRY_TO_DATE({{ col_name }}, 'DD-MM-YYYY')

        -- Likely MM-DD-YYYY (e.g., 04-13-2024)
        WHEN TRY_CAST(SPLIT_PART({{ col_name }}, '-', 2) AS INTEGER) > 12
          AND TRY_CAST(SPLIT_PART({{ col_name }}, '-', 1) AS INTEGER) <= 12
        THEN TRY_TO_DATE({{ col_name }}, 'MM-DD-YYYY')

        -- Known formats
        WHEN {{ col_name }} LIKE '%/%' 
        THEN COALESCE(
            TRY_TO_DATE({{ col_name }}, 'DD/MM/YYYY'),
            TRY_TO_DATE({{ col_name }}, 'YYYY/MM/DD')
        )

        -- Fallback: already in YYYY-MM-DD or other clean formats
        ELSE TRY_TO_DATE({{ col_name }}, 'YYYY-MM-DD')
    END
{% endmacro %}



-- Format	        Recognized As	        Output Example
-- '13-04-2024'	    DD-MM-YYYY	            2024-04-13
-- '04-13-2024'	    MM-DD-YYYY	            2024-04-13
-- '02/04/2024'	    DD/MM/YYYY	            2024-04-02
-- '2024-04-02'	    YYYY-MM-DD	            2024-04-02
-- Invalid string	Fails gracefully	    NULL