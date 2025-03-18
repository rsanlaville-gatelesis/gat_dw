{# model variables #}
{% set dl = 365 * 6 %} 
{% set k = 0.01 %}
{% set w = 2 %}
{% set dr = 365 * 4 %}


with

----- get all data points

points as (

    ----- SO
    select 
      'SO' source
    , pnm_auto_key
    , pcc_auto_key
    , entry_date date
    , unit_price value
    , _fivetran_synced 
    from {{ source('qctl','so_detail') }}
    where route_code = 'S'
      and entry_date >= dateadd(day, -{{dl}}, current_date)
      and qty_invoiced > 0
      and unit_price > 0

    union all 
    
    ----- PO
    select 
      'PO' source
    , pnm_auto_key
    , pcc_auto_key
    , entry_date date
    , unit_cost value
    , _fivetran_synced 
    from {{ source('qctl','po_detail') }}
    where route_code = 'S'
      and entry_date >= dateadd(day, -{{dl}}, current_date)
      and qty_rec > 0
      and unit_cost > 0

    union all 

    ----- CQ
    select 
      'CQ' source
    , pnm_auto_key
    , pcc_auto_key
    , entry_date date
    , unit_price value
    , _fivetran_synced
    from {{ source('qctl','cq_detail') }}
    where route_code = 'S'
      and entry_date >= dateadd(day, -{{dl}}, current_date)
      and qty_quoted > 0
      and unit_price > 0

    union all 

    ----- VQ
    select 
      'VQ' source
    , pnm_auto_key
    , pcc_auto_key
    , entry_date date
    , unit_cost value
    , _fivetran_synced
    from {{ source('qctl','vq_detail') }} vqd
    left join {{ ref('stg_cmp_vq_score') }} score
      on score.cmp_auto_key = vqd.cmp_auto_key
    where route_code = 'S'
      and entry_date >= dateadd(day, -{{dl}}, current_date)
      and qty_quoted > 0
      and unit_cost > 0
      and coalesce(score.gap,0) < 50 -- removes companies quoting 50x higher than average
      and coalesce(score.cmp_ranking, 999) > 3 -- removes worst 3 companies
),


stats_pn_pc as (
  SELECT
    avg(value)    average_pn_pc
  , stddev(value) std_dev_pn_pc
  , median(value) median_pn_pc
  , count(value) count_data_points_pn_pc
  , pnm_auto_key
  , pcc_auto_key
  from points
  group by pnm_auto_key, pcc_auto_key
),

stats_pn as (
  SELECT
    avg(value)    average_pn
  , stddev(value) std_dev_pn
  , median(value) median_pn
  , count(value) count_data_points_pn
  , pnm_auto_key
  from points
  group by pnm_auto_key
),

final as (
  select 
    points.*
  , stats_pn_pc.median_pn_pc
  , stats_pn_pc.count_data_points_pn_pc
  , abs(points.value - stats_pn_pc.median_pn_pc) mad_pn_pc
  , case when stats_pn_pc.std_dev_pn_pc <> 0 then abs((points.value - stats_pn_pc.average_pn_pc) / stats_pn_pc.std_dev_pn_pc) end z_score_pn_pc
  , stats_pn.median_pn
  , stats_pn.count_data_points_pn
  , abs(points.value - stats_pn.median_pn) mad_pn
  , case when stats_pn.std_dev_pn <> 0 then abs((points.value - stats_pn.average_pn) / stats_pn.std_dev_pn) end z_score_pn
  
  from points
  left join stats_pn_pc
    on stats_pn_pc.pnm_auto_key = points.pnm_auto_key
   and stats_pn_pc.pcc_auto_key = points.pcc_auto_key
    left join stats_pn
    on stats_pn.pnm_auto_key = points.pnm_auto_key
),

mad_pn_pc as
(
  select 
    final.pnm_auto_key
  , final.pcc_auto_key
  , median(abs(final.value - stats_pn_pc.median_pn_pc)) mad_pn_pc
  from final 
  left join stats_pn_pc
    on stats_pn_pc.pnm_auto_key = final.pnm_auto_key
   and stats_pn_pc.pcc_auto_key = final.pcc_auto_key
 group by final.pnm_auto_key
        , final.pcc_auto_key
),

mad_pn as
(
  select 
    final.pnm_auto_key
  , median(abs(final.value - stats_pn.median_pn)) mad_pn
  from final 
  left join stats_pn
    on stats_pn.pnm_auto_key = final.pnm_auto_key
 group by final.pnm_auto_key
),

final_w as (
  select 
    source
  , final.pnm_auto_key
  , final.pcc_auto_key
  , case when source in ('VQ','CQ') then 1 else {{ w }} end w
  , value
  , date
  , median_pn_pc
  , count_data_points_pn_pc
  , z_score_pn_pc
  , case when mad_pn_pc.mad_pn_pc <> 0 then  0.6745 * (abs(value - median_pn_pc)) / mad_pn_pc.mad_pn_pc end modded_z_score_pn_pc
  , median_pn
  , count_data_points_pn
  , z_score_pn
  , case when mad_pn.mad_pn <> 0 then  0.6745 * (abs(value - median_pn)) / mad_pn.mad_pn end modded_z_score_pn
  from final
  left join mad_pn_pc
    on final.pnm_auto_key = mad_pn_pc.pnm_auto_key
    and final.pcc_auto_key = mad_pn_pc.pcc_auto_key
    left join mad_pn
    on final.pnm_auto_key = mad_pn.pnm_auto_key
),

final_ww as (
  select *
  , w * {{ dr }} * exp(-{{ k }} * datediff(day, date, current_date)) 
      - datediff(day,date, current_date) * exp(-{{ k }}*{{ dr }}) exp_weight
  from final_w
  -- where z_score < 2.33
  --  and modded_z_score < 3.5
  )
  
select 
    source
  , pnm_auto_key
  , pcc_auto_key
  , value
  , date
  , z_score_pn_pc
  , modded_z_score_pn_pc
  , count_data_points_pn_pc
  , z_score_pn
  , modded_z_score_pn
  , count_data_points_pn
  , case when exp_weight < 0 then 0 else exp_weight end weight
from final_ww


-- {% if is_incremental() %}
-- where _fivetran_synced >= (select coalesce(max(_fivetran_synced),'1900-01-01') from {{ this }} )
-- {% endif %}









