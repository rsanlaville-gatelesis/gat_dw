
{% macro generate_qtm_union_model(table_name) %}

    {% set schemas = var('qtm_schemas') %}
    {% set qtm_tables = [] %}

    {% for schema in schemas %}
        {% do qtm_tables.append(source(schema, table_name)) %}
    {% endfor %}

    {{ dbt_utils.union_relations(qtm_tables, source_column_name = 'SRC_TABLE') }}
    
{% endmacro %}