{% set dl = 2 * 365 %}
{% set z_limit = 2.33 %}
{% set mz_limit = 3.5 %}

with datapoints as 
(
  select
    pnm_auto_key
  , value 
  , date
  , count_data_points_pn
  , weight
  from {{ ref('stg_fmv_raw') }}
  where date > dateadd(day, - {{ dl }}, current_date)
    and pcc_auto_key in (1,2,67,5,4)
    and z_score_pn < {{ z_limit }}
    and modded_z_score_pn < {{ mz_limit }}
)

select
  pnm.pn
, dp.pnm_auto_key
, count(dp.value) COUNT_DATA_POINTS_pn
, case
    when sum(dp.weight) != 0
    then round(sum(dp.value * dp.weight) / sum(dp.weight),2)
  end fmv
from datapoints dp
left join {{ source('qctl','parts_master') }} pnm
 on to_number(dp.pnm_auto_key) = to_number(pnm.pnm_auto_key)
group by
  pnm.pn
, dp.pnm_auto_key