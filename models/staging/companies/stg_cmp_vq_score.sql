with vq as (
select 
  pcc_auto_key
, pnm_auto_key
, unit_cost
from {{ source('qctl','vq_detail') }} 
where qty_quoted > 0
  and unit_cost > 0 
  and cmp_auto_key is not null
  and route_code = 'S'
  and entry_date >= dateadd(day, -365, current_date())
),
vq_stats as
(
select
  pcc_auto_key
, pnm_auto_key
,  avg(unit_cost) average 
, stddev(unit_cost) stddev
, count(unit_cost) count_data_points
from vq
group by 
  pcc_auto_key
, pnm_auto_key
),
vq_clean as 
(
select *
from (
    select 
      vqd.cmp_auto_key
    , vqd.pcc_auto_key
    , vqd.pnm_auto_key
    , vqd.unit_cost
    , case when vqs.stddev!=0 and vqs.stddev is not null then abs((vqd.unit_cost - vqs.average) / vqs.stddev) end z_score
    from {{ source('qctl','vq_detail') }} vqd
    left join vq_stats vqs
      on  vqd.pcc_auto_key = vqs.pcc_auto_key
      and vqd.pnm_auto_key = vqs.pnm_auto_key
    where vqd.qty_quoted > 0
      and vqd.unit_cost > 0 
      and route_code = 'S'
      and vqd.cmp_auto_key is not null
      and vqd.entry_date >= dateadd(day, -365, current_date())
    )
where z_score <= 2.33
),
vq_averages as
(
      select
        vqd.cmp_auto_key 
      , vqd.pcc_auto_key
      , vqd.pnm_auto_key
      , avg(vqd.unit_cost) avg_cost
      , count(vqd.UNIT_COST) count_data_points
      from vq_clean vqd
      group by     
        vqd.cmp_auto_key 
      , vqd.pcc_auto_key
      , vqd.pnm_auto_key
),
main as
(
      select distinct
        vqd.cmp_auto_key 
      , vqd.pcc_auto_key
      , vqd.pnm_auto_key
      from vq_clean vqd
),
cmp_comp as 
(
    
    -------------
    select
      cmp.COMPANY_NAME
    , cmp.cmp_auto_key
    , pnm.pn
    , pcc.CONDITION_CODE 
    , final.avg_cost
    , final.count_data_points_this
    , final.count_data_points_other
    , final.global_ave
    , (final.avg_cost - final.global_ave) / final.global_ave gap
    from(
      select
        main.cmp_auto_key 
      , main.pcc_auto_key
      , main.pnm_auto_key
      , cur.avg_cost
      , cur.count_data_points count_data_points_this
      , sum(ave.count_data_points) count_data_points_other
      , sum(ave.avg_cost * ave.count_data_points) / sum(ave.count_data_points) global_ave
      from main
      left join vq_averages cur
        on  cur.cmp_auto_key = main.cmp_auto_key
        and cur.pcc_auto_key = main.pcc_auto_key
        and cur.pnm_auto_key = main.pnm_auto_key
      left join vq_averages ave
        on  ave.cmp_auto_key <> main.cmp_auto_key
        and ave.pcc_auto_key = main.pcc_auto_key
        and ave.pnm_auto_key = main.pnm_auto_key
      group by
        main.cmp_auto_key 
      , main.pcc_auto_key
      , main.pnm_auto_key
      , cur.avg_cost
      , cur.count_data_points
      ) final
    left join {{ source('qctl','companies') }} cmp
      on final.cmp_auto_key = cmp.CMP_AUTO_KEY 
    left join {{ source('qctl','parts_master') }} pnm
      on final.pnm_auto_key = pnm.PNM_AUTO_KEY 
    left join {{ source('qctl','part_condition_codes') }} pcc
      on final.pcc_auto_key = pcc.pcc_AUTO_KEY 
    where final.count_data_points_other >= 5
),

cmp_over_10 as (
  select 
    cmp_auto_key
  , company_name
  , round(avg(GAP) * 100, 2) gap
  , count(*) count_data_points
  from cmp_comp
  group by  cmp_auto_key, company_name
  having count(*) > 10
  order by 3 desc)

select 
  * 
, row_number() over (order by gap desc) cmp_ranking
from cmp_over_10
