{% macro centralmacro_scd(table_name, column_data, max_load_date, is_first_load) %}
    {% set select_clauses = [] %}
 
    -- {# Transformation flags #}
    -- {% set tf_query = "SELECT COLUMNNAME, TRANSFORMATION_FLAG FROM ACCELERATOR_SANDBOX.RAW_FINAL.TRANSFORMATION_FLAG_TABLE WHERE TABLENAME = '" ~ table_name ~ "'" %}

    {% set tf_table = source('accelerator_sandbox', 'TRANSFORMATION_FLAG_TABLE') %}
    {% set tf_query = "SELECT COLUMNNAME, TRANSFORMATION_FLAG FROM " ~ tf_table ~ " WHERE TABLENAME = '" ~ table_name ~ "'" %}
    {% set tf_result = run_query(tf_query) %}

    {% set tf_dict = {} %}
    {% for row in tf_result.rows %}
        {% set col_lower = row[0] | lower %}
        {% set flag = row[1] %}
        {% do tf_dict.update({ col_lower: flag }) %}
    {% endfor %}
 
    {# Loop over all columns #}
    {% for row in column_data %}
        {% set col_name = row[0] %}
        {% set col_type = row[1] | lower %}
        {% set col_lower = col_name | lower %}
        {% set quoted_col = '"' ~ col_name ~ '"' %}
        {% set len = col_lower | length %}
        {% set tf_val = tf_dict.get(col_lower, none) %}
 
        {% if tf_val == true %}
            {% if col_type.startswith('timestamp') or (len >= 3 and col_lower[(len-3):] == '_id') %}
                {% do select_clauses.append('TRIM(' ~ quoted_col ~ ') AS ' ~ quoted_col) %}
            {% elif col_lower == 'recordno' %}
                {% do select_clauses.append(clean_number(quoted_col, col_type) ~ ' AS ' ~ quoted_col) %}
            {% elif 'email' in col_lower %}
                {% do select_clauses.append(clean_email(quoted_col) ~ ' AS ' ~ quoted_col) %}
            {% elif 'phone' in col_lower or 'mobile' in col_lower or 'contact' in col_lower %}
                {% do select_clauses.append(clean_phone(quoted_col) ~ ' AS ' ~ quoted_col) %}
            {% elif col_type == 'number' %}
                {% do select_clauses.append(clean_number(quoted_col, col_type) ~ ' AS ' ~ quoted_col) %}
            {% elif 'code' in col_lower %}
                {% do select_clauses.append(clean_code(quoted_col) ~ ' AS ' ~ quoted_col) %}
            {% elif col_type == 'date' %}
                {% do select_clauses.append(clean_date(quoted_col) ~ ' AS ' ~ quoted_col) %}
            {% elif 'url' in col_lower %}
                {% do select_clauses.append(clean_url(quoted_col) ~ ' AS ' ~ quoted_col) %}
            {% else %}
                {% do select_clauses.append(clean_text(quoted_col) ~ ' AS ' ~ quoted_col) %}
            {% endif %}
        {% else %}
            {% do select_clauses.append(quoted_col) %}
        {% endif %}
    {% endfor %}
 
    {# Add SCD fields #}
    {% do select_clauses.append('CAST(LOAD_DATE AS TIMESTAMP) AS START_DATE') %}
    {% do select_clauses.append('CAST(NULL AS TIMESTAMP) AS END_DATE') %}
 
    {% set final_select = select_clauses | join(',\n    ') %}
 
    (
    SELECT
        {{ final_select }}
    FROM RAW_FINAL."{{ table_name }}"
    WHERE CAST(LOAD_DATE AS DATE) = (
    SELECT MAX(CAST(LOAD_DATE AS DATE)) FROM RAW_FINAL."{{ table_name }}"
    )
    -- AND COALESCE(RECORDSTATUS, '') != 'd'
    )
{% endmacro %}