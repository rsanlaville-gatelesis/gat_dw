with older_lots_all as (
  select 
    str.STM_AUTO_KEY
  , str.WOO_AUTO_KEY
  , str.qty_reserved
  , row_number() over (partition by str.STM_AUTO_KEY  order by str.entry_date desc) lot_num
  from {{ source('qctl','stock_reservations') }} str
  inner join {{ source('qctl','wo_operation') }} woo
    on woo.WOO_AUTO_KEY = str.woo_auto_key
  where str.WOO_AUTO_KEY is not null
    and str.STM_AUTO_KEY is not null
    and lower(woo.WO_TYPE) = 'lot'
    and str.qty_reserved = 0),
older_lots as (
  select
    WOO_AUTO_KEY
  , STM_AUTO_KEY LOT_STM_AUTO_KEY
  , QTY_RESERVED
  , 3 QUERY_NUM
  from older_lots_all
  where lot_num = 1
),
stm_to_wo_rez as (
  -- this will bring only open Lots.
  SELECT
       WOO_AUTO_KEY
     , STM_AUTO_KEY LOT_STM_AUTO_KEY
     , QTY_RESERVED
     , 1 QUERY_NUM
  FROM  {{ source('qctl','stock_reservations') }} 
     WHERE WOO_AUTO_KEY IS NOT NULL
      AND QTY_RESERVED > 0 
       and _fivetran_deleted = false
  -- this brings all units under RO linked to a WO
    UNION
  SELECT
   ROD.WOO_AUTO_KEY
  , STR.STM_AUTO_KEY LOT_STM_AUTO_KEY
  , STR.QTY_RESERVED
  , 2 QUERY_NUM
  FROM {{ source('qctl','stock_reservations') }} STR
     LEFT JOIN {{ source('qctl','ro_detail') }} ROD
       ON ROD.ROD_AUTO_KEY = STR.ROD_AUTO_KEY
       and rod._fivetran_deleted = false
  WHERE ROD.QTY_RESERVED > 0 
   AND ROD.WOO_AUTO_KEY IS NOT NULL
   and str._fivetran_deleted = false
  -- and this brings all units previously linked to lot WO's
    UNION
  select * 
  from older_lots
   ),
V as (
  SELECT 
     WOO_AUTO_KEY
   , LOT_STM_AUTO_KEY
   , QTY_RESERVED
   , ROW_NUMBER() OVER (PARTITION BY WOO_AUTO_KEY ORDER BY QUERY_NUM) ROW_NUM
   FROM stm_to_wo_rez
   ),
final as (
SELECT
  V.WOO_AUTO_KEY
, V.LOT_STM_AUTO_KEY
, STM.RECEIVER_NUMBER
, STM.ORIGINAL_PO_NUMBER
, pnm.pn
, pnm.description
, ptc.pn_type_code
, STM.UNIT_COST
, pod.entry_date pod_entry_date
, woo.entry_date woo_entry_date
, pod.unit_cost * pod.QTY_REC orig_po_cost
, woo.open_flag
, woo.si_number
FROM V
LEFT JOIN {{ source('qctl','wo_operation') }} WOO
on woo.woo_AUTO_KEY = v.woo_auto_key
  and woo._FIVETRAN_DELETED = false
LEFT JOIN {{ source('qctl','stock') }} STM
  ON STM.STM_AUTO_KEY = V.LOT_STM_AUTO_KEY
  and STM._fivetran_deleted = false
LEFT JOIN {{ source('qctl','po_detail') }} POD
  on pod.POD_AUTO_KEY = stm.pod_auto_key
  and pod._FIVETRAN_DELETED = false
LEFT JOIN  {{ source('qctl','parts_master') }} PNM
  on pnm.pnm_AUTO_KEY = stm.pnm_auto_key
LEFT JOIN  {{ source('qctl','pn_type_codes') }} PTC
  on PTC.PTC_AUTO_KEY = pnm.PTC_auto_key
WHERE ROW_NUM = 1)
--
select *
from final