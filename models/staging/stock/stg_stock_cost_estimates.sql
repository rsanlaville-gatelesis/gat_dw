
  
with 

ro as (
  select
    pnm_auto_key
  , avg(labor_cost) ro_cost
  from {{ source('qctl','ro_detail')}}
  where last_delivery_date > dateadd(day, -365, current_date)
  group by pnm_auto_key
),

-- al
stm_pre_lot as (  
  select 
    stm.stm_auto_key
  , stm.pnm_auto_key
  , stm.qty_oh
  , stm.unit_cost
  , stm.unit_price
  , stm.stm_lot
  , fmv.fmv
  , cnc.comm_rate
  , coalesce(cnc.comm_rate / 100 * stm.unit_price, 0) cons_comm_cost
  , stm.qty_oh * coalesce(iff(stm.unit_price!=0,stm.unit_price,null), fmv.fmv, 0) lot_cost_spread_key
  -- TODO: we don't want to the main lot items to have a lot spread.
  , pcc.cond_level
  , ro.ro_cost
  , case
      when pcc.cond_level >= 3 then coalesce(ro.ro_cost,0)
      else 0
    end ro_cost_needed
  from {{ source('qctl','stock') }} stm
  left join {{ ref('stg_fmv_pn') }} fmv
    on fmv.pnm_auto_key = stm.pnm_auto_key
  left join {{ source('qctl','consignment_codes')}} cnc
    on cnc.cnc_auto_key = stm.cnc_auto_key
  left join {{ source('qctl','part_condition_codes')}} pcc
    on pcc.pcc_auto_key = stm.pcc_auto_key
  left join ro
    on ro.pnm_auto_key = stm.pnm_auto_key
  where stm.qty_oh > 0
    and stm._fivetran_deleted = false
),

-- pull all the lots main asset stock lines to get their cost
lots as (
  select
    lot_stm_auto_key
  , unit_cost lot_cost
  from {{ ref('stg_lot_stm')}}
),

stm_with_lot as (
  select
    stm_pre_lot.*
  , lots.lot_cost
  , case
      when lots.lot_stm_auto_key = stm_pre_lot.stm_auto_key then true
      else false
    end is_lot
  from stm_pre_lot
  left join lots on lots.lot_stm_auto_key = stm_pre_lot.stm_lot
),

stm_adjusted_spread as (
  select 
    *
  , case
      when is_lot = true then 0
      else lot_cost_spread_key
    end lot_cost_spread_key_adjusted
  , case
      when is_lot = true then 0
      else ro_cost_needed
    end ro_cost_needed_adjusted
  from stm_with_lot
),

stm_with_totals as (
  select
    *
  , sum(ro_cost_needed_adjusted) over (partition by stm_lot) total_lot_ro_cost_needed
  , sum(lot_cost_spread_key_adjusted) over (partition by stm_lot) total_spread
  from stm_adjusted_spread
),

stm as (
  select 
    *
  , case
      when total_spread != 0
      then round(coalesce(lot_cost_spread_key,0) / total_spread * (lot_cost + coalesce(total_lot_ro_cost_needed,0)),2)
    end lot_cost_spread
  from stm_with_totals
)

select * 
from stm