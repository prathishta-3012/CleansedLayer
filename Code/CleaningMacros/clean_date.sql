{% macro clean_date(col_name) %}
    TO_DATE({{ col_name }})
{% endmacro %}

-- Raw Value	            Output	        Notes
-- '2024-04-02'	            2024-04-02	    Standard ISO date
-- '2024-04-02 13:45:00'	2024-04-02	    Time is dropped by TO_DATE()
-- NULL	                    NULL	        Preserved