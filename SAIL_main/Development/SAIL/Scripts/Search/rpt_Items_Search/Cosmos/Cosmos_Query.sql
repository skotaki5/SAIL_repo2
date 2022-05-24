-- rpt_Items_Search

--Result Set 1

/*
Parameter Requirement info -
->@DPProductLineKey   required
@DPServiceLineKey   optional
@SearchBy           optional
@SearchValue        optional
@WarehouseId        optional
@warehouse          optional
@BatchStatus        optional
@ReasonCode         optional

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
(c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId =@DPProductLineKey )
and (c.DP_SERVICELINE_KEY in (@AccountKeys.DPServiceLineKey) or c.DP_SERVICELINE_KEY =@DPServiceLineKey)
and c.FacilityId in (@warehouse.warehouseID)
and c.warehouseCode in (@warehouse.warehouseName)
and ( (c.itemNumber like '%@SearchValue%')  --@VarSearchBy = 'ITEMNUMBER'
or (c.itemDescription like '%@SearchValue%') --@VarSearchBy = 'ITEMDESCRIPTION'
or (c.Designator like '%@SearchValue%') --@VarSearchBy = 'DESIGNATOR'
or (c.LPNNumber like '%@SearchValue%') --@VarSearchBy = 'LPN'
or (c.VendorSerialNumber like '%@SearchValue%') --@VarSearchBy = 'VSN'
or (c.VendorLotNumber like '%@SearchValue%') --@VarSearchBy = 'VCL'
or (c.VendorLotNumber like '%@SearchValue%') --@VarSearchBy = 'INVENTORYBATCHNUMBER'
or (c.InvRef1 like '%@SearchValue%' --@VarSearchBy = 'INBOUNDREFERENCENUMBER'
or c.InvRef2 like '%@SearchValue%'
or c.InvRef3 like '%@SearchValue%'
or c.InvRef4 like '%@SearchValue%'
or c.InvRef5 like '%@SearchValue%'))
and (is_null(c.FacilityId)?'':c.FacilityId) <> '' and c.is_deleted = 0 
and c.BatchStatus in (@BatchStatus) --null for given accountid in both ssms and cosmos
and c.BatchHoldReason in (@ReasonCode) --null for given accountid in both ssms and cosmos