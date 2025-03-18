{% macro src_schema(src_table) %}
  cast(case
    when lower({{ src_table }}) like '%quantum_prod_qctl%' then 'QCTL'
    when lower({{ src_table }}) like '%quantum_prod_gatcrgqctl%' then 'GATCRGQCTL'
    when lower({{ src_table }}) like '%quantum_prod_gatcrg%' then 'GATCRGSE'
    when lower({{ src_table }}) like '%quantum_prod_gatrs%' then 'GATRS'
    else 'UNDEFINED'
  end as varchar(10)
  )
{% endmacro %}