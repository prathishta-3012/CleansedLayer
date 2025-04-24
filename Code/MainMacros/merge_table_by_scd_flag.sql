{% macro merge_table_by_scd_flag() %}
    -- {% set result = run_query("SELECT TABLE_NAME, PRIMARY_KEYS, SCD_FLAG FROM ACCELERATOR_SANDBOX.RAW_FINAL.SCD_MAPPING_TABLE") %}

    {% set mapping_table = source('accelerator_sandbox', 'SCD_MAPPING_TABLE_TEST') %}
    {% set result = run_query("SELECT TABLE_NAME, PRIMARY_KEYS, SCD_FLAG FROM " ~ mapping_table) %}

    {% for row in result.rows %}
        {% set table_name = row[0] %}
        {% set pk_raw = row[1] %}
        {% set is_scd = row[2] %}

        {% if not pk_raw %}
            {% do log("❌ Skipping " ~ table_name ~ " — no PRIMARY_KEYS found", info=True) %}
            {% continue %}
        {% endif %}

        {% set latest_load_date_query %}
            SELECT MAX(LOAD_DATE) FROM RAW_FINAL."{{ table_name }}"
        {% endset %}
        {% set latest_load_date_result = run_query(latest_load_date_query) %}
        {% set latest_load_date = latest_load_date_result.rows[0][0] if latest_load_date_result and latest_load_date_result.rows[0] else none %}

        {% set exists_check_query %}
            SELECT COUNT(*) FROM CLEANSED."{{ table_name }}" WHERE LOAD_DATE = '{{ latest_load_date }}'
        {% endset %}
        {% set exists_result = run_query(exists_check_query) %}
        {% set existing_count = exists_result.rows[0][0] if exists_result and exists_result.rows[0] else 0 %}

        {% if existing_count > 0 %}
            {% do log("⏩ Skipping " ~ table_name ~ " — already loaded for LOAD_DATE " ~ latest_load_date, info=True) %}
            {% continue %}
        {% endif %}

        {% set pk_list = pk_raw.split(',') | map('trim') | list %}
        {% do log("🚀 Starting merge for table: " ~ table_name ~ " (SCD=" ~ is_scd ~ ")", info=True) %}

        {% set columns_query %}
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'RAW_FINAL'
              AND table_name = '{{ table_name }}'
            ORDER BY ordinal_position
        {% endset %}
        {% set column_data = run_query(columns_query).rows %}
        {% if column_data | length == 0 %}
            {% do log("⚠️ Skipping table " ~ table_name ~ " — no columns found.", info=True) %}
            {% continue %}
        {% endif %}

        {% set all_cols = column_data | map(attribute=0) | list %}
        {% set select_cols = all_cols | list %}
        {% if is_scd %}
            {% do select_cols.append('START_DATE') %}
            {% do select_cols.append('END_DATE') %}
        {% endif %}
        {% set select_list = select_cols | join(', ') %}
        
        {% set source_cte = centralmacro_scd(table_name, column_data, none, is_scd) %}

        {% set join_conditions = [] %}
        {% for pk in pk_list %}
            {% do join_conditions.append('"EXISTING"."' ~ pk ~ '" = "NEW"."' ~ pk ~ '"') %}
        {% endfor %}
        {% set join_clause = join_conditions | join(' AND ') %}
        {% if join_clause == '' %}
            {% do log("❌ Skipping " ~ table_name ~ " — join_clause is empty", info=True) %}
            {% continue %}
        {% endif %}

        {% if is_scd %}
            {% set col_type_map = {} %}
            {% for col in column_data %}
                {% do col_type_map.update({ col[0]: col[1] | lower }) %}
            {% endfor %}

            {% set non_key_cols = all_cols | reject("in", pk_list) | list %}
            {% set change_conditions = [] %}
            {% for col in non_key_cols %}
                {% set dtype = col_type_map.get(col, 'varchar') %}
                {% if 'number' in dtype %}
                    {% set default = '0' %}
                {% elif 'boolean' in dtype %}
                    {% set default = 'FALSE' %}
                {% elif 'date' in dtype or 'timestamp' in dtype %}
                    {% set default = "'1970-01-01'" %}
                {% else %}
                    {% set default = "''" %}
                {% endif %}
                {% do change_conditions.append('COALESCE("EXISTING"."' ~ col ~ '", ' ~ default ~ ') <> COALESCE("NEW"."' ~ col ~ '", ' ~ default ~ ')') %}
            {% endfor %}
            {% set change_clause = change_conditions | join(' OR ') %}
            {% if change_clause == '' %}
                {% do log("⚠️ Skipping table " ~ table_name ~ " — no non-key fields to compare", info=True) %}
                {% continue %}
            {% endif %}

            {% set merge_sql %}
-- Handle deleted records for SCD Type 2 tables
MERGE INTO CLEANSED."{{ table_name }}" AS "EXISTING"
USING (
    SELECT 
        {{ pk_list | join(', ') }},
        CAST(LOAD_DATE AS TIMESTAMP) AS LOAD_DATE
    FROM RAW_FINAL."{{ table_name }}"
    WHERE CAST(LOAD_DATE AS DATE) = (
        SELECT MAX(CAST(LOAD_DATE AS DATE)) FROM RAW_FINAL."{{ table_name }}"
    )
    AND RECORDSTATUS = 'd'
) AS "DELETED"
ON {{ join_conditions | map('replace', '"NEW"', '"DELETED"') | join(' AND ') }} AND "EXISTING"."END_DATE" IS NULL
WHEN MATCHED THEN 
    UPDATE SET "END_DATE" = "DELETED".LOAD_DATE, "RECORDSTATUS" = 'd';

-- Handle normal SCD updates
MERGE INTO CLEANSED."{{ table_name }}" AS "EXISTING"
USING ({{ source_cte }}) AS "NEW"
ON {{ join_clause }} AND "EXISTING"."END_DATE" IS NULL
WHEN MATCHED AND ({{ change_clause }}) THEN
    UPDATE SET "END_DATE" = "NEW"."START_DATE";

INSERT INTO CLEANSED."{{ table_name }}" ({{ select_list }})
SELECT {{ select_list }}
FROM ({{ source_cte }}) AS "NEW"
WHERE NOT EXISTS (
    SELECT 1 FROM CLEANSED."{{ table_name }}" AS "EXISTING"
    WHERE {{ join_clause }}
    AND "START_DATE" = "NEW"."START_DATE"
)
AND (
    EXISTS (
        SELECT 1 FROM CLEANSED."{{ table_name }}" AS "EXISTING"
        WHERE {{ join_clause }} AND "END_DATE" = "NEW"."START_DATE"
    ) OR NOT EXISTS (
        SELECT 1 FROM CLEANSED."{{ table_name }}" AS "EXISTING"
        WHERE {{ join_clause }}
    )
);
            {% endset %}
        {% else %}
            {% set update_clauses = [] %}
            {% for col in all_cols %}
                {% if col | trim not in pk_list %}
                    {% do update_clauses.append('"' ~ col ~ '" = "NEW"."' ~ col ~ '"') %}
                {% endif %}
            {% endfor %}
            {% set update_clause = update_clauses | join(', ') %}

            {% set merge_sql %}
-- Handle deleted records for non-SCD tables
DELETE FROM CLEANSED."{{ table_name }}" AS "EXISTING"
WHERE EXISTS (
    SELECT 1
    FROM RAW_FINAL."{{ table_name }}" AS "DELETED"
    WHERE CAST(LOAD_DATE AS DATE) = (
        SELECT MAX(CAST(LOAD_DATE AS DATE)) FROM RAW_FINAL."{{ table_name }}"
    )
    AND RECORDSTATUS = 'd'
    AND {{ join_conditions | map('replace', '"NEW"', '"DELETED"') | join(' AND ') }}
);

-- Handle normal non-SCD updates
MERGE INTO CLEANSED."{{ table_name }}" AS "EXISTING"
USING ({{ source_cte }}) AS "NEW"
ON {{ join_clause }}
WHEN MATCHED THEN UPDATE SET {{ update_clause }}
WHEN NOT MATCHED THEN INSERT ({{ select_list }}) VALUES ({{ select_list }});
            {% endset %}
        {% endif %}

        {% do log("📦 Executing MERGE for table: " ~ table_name, info=True) %}
        {% do run_query(merge_sql) %}
        {% do log("✅ MERGE completed for table: " ~ table_name, info=True) %}
    {% endfor %}
{% endmacro %}