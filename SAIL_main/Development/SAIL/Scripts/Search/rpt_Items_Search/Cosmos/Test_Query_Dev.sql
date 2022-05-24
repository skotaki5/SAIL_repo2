-- rpt_Items_Search

--Result Set 1

/*
Target Container - digital_summary_inventory
*/

select 
c.itemNumber,
c.itemDescription,
(SELECT DISTINCT IS_NULL(c.VendorLotNumber)?'':c.VendorLotNumber AS batchNumber
,IS_NULL(c.BatchStatus)?'':c.BatchStatus AS batchStatus,
IS_NULL(c.BatchHoldReason)?'':c.BatchHoldReason AS ReasonCode,
c.ExpirationDate AS expirationDate from c) as batch_details,
c.warehouseCode
from c
where 
(c.AccountId in ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529'))
and (c.DP_SERVICELINE_KEY in ("A0A885C1-8A23-4218-A7A0-F7236ADBF4AD"))
and c.FacilityId in ("E4F65B8F-C501-47F8-A226-8E50E46EEAA2")
and c.warehouseCode in ("WCMH1")
and ( (c.itemNumber like '%NT0H11BC%') 
or (c.itemDescription like '%NT0H11BC%') 
or (c.Designator like '%NT0H11BC%') 
or (c.LPNNumber like '%NT0H11BC%') 
or (c.VendorSerialNumber like '%NT0H11BC%')
or (c.VendorLotNumber like '%NT0H11BC%')
or (c.VendorLotNumber like '%NT0H11BC%') 
or (c.InvRef1 like '%NT0H11BC%' 
or c.InvRef2 like '%NT0H11BC%'
or c.InvRef3 like '%NT0H11BC%'
or c.InvRef4 like '%NT0H11BC%'
or c.InvRef5 like '%NT0H11BC%'))
and (is_null(c.FacilityId)?'':c.FacilityId) <> '' and c.is_deleted = 0 
--and c.BatchStatus in (@BatchStatus) --null for given accountid in both ssms and cosmos
--and c.BatchHoldReason in (@ReasonCode) --null for given accountid in both ssms and cosmos