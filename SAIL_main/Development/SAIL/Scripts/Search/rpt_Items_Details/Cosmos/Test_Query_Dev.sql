-- rpt_Items_Details

-- Result Set 1

/*
Target Container - digital_summary_inventory
*/

select 
a.itemNumber,
a.itemDescription,
a.hazardClass,
a.itemDimensions_length,
a.itemDimensions_width,
a.itemDimensions_height,
a.itemDimensions_unitOfMeasurement,
a.itemWeight_weight,
a.itemWeight_unitOfMeasurement,
a.warehouseId,
a.warehouseCode,
MAX(a.availableQuantity) availableQuantity,
a.inventoryReasonCode_nonAvailableQuantity,
MAX(a.nonAvailableQuantity) nonAvailableQuantity,
a.Designator,
a.LPN,
a.VSN,
a.VCL, --
a.referenceNumber1,
a.referenceNumber2,
a.referenceNumber3,
a.referenceNumber4,
a.referenceNumber5,
MAX(a.availableQuantity) onHandQuantity,
a.HazmatClass,
a.StrategicGoodsFlag,
a.UNCode, 
a.batchNumber,
a.expirationDate,
a.batchStatus,
a.reasonCode = 'NA'? null : a.reasonCode reasonCode,
a.Account 
from
(select 
c.itemNumber,
c.itemDescription,
c.hazardClass,
c.itemDimensions_length,
c.itemDimensions_width,
c.itemDimensions_height,
c.itemDimensions_unitOfMeasurement_code itemDimensions_unitOfMeasurement,
c.itemWeight_weight,
c.itemWeight_unitOfMeasurement_Code itemWeight_unitOfMeasurement,
c.FacilityId warehouseId,
c.warehouseCode,
c.availableQuantity,
(SELECT IS_NULL(c.HoldDescription)?'':c.HoldDescription as nonAvailabilityReasonCode) AS inventoryReasonCode_nonAvailableQuantity,
c.nonAvailableQuantity,
Array(SELECT IS_NULL(c.Designator)?'':c.Designator as Designator) AS Designator,
Array(SELECT IS_NULL(c.LPNNumber)?'':c.LPNNumber as LPN) AS LPN,
Array(SELECT IS_NULL(c.VendorSerialNumber)?'':c.VendorSerialNumber as VSN) AS VSN,
Array(SELECT IS_NULL(c.VendorLotNumber)?'':c.VendorLotNumber as VCL) AS VCL, --
c.InvRef1 referenceNumber1,
c.InvRef2 referenceNumber2,
c.InvRef3 referenceNumber3,
c.InvRef4 referenceNumber4,
c.InvRef5 referenceNumber5,
c.HazmatClass,
c.StrategicGoodsFlag,
c.UNNumber UNCode, 
'Y'='Y'? c.VendorLotNumber : null batchNumber, --@isBatchInformationRequired='Y'
'Y'='Y'?c.ExpirationDate : null expirationDate, --@isBatchInformationRequired='Y'
'Y'='Y'?c.BatchStatus : null batchStatus, --@isBatchInformationRequired='Y'
'Y'='Y'? (c.BatchStatus = 'Released'? 'NA':c.BatchHoldReason) : null reasonCode, --@isBatchInformationRequired='Y'
c.Account_number Account
from c
where 
(c.AccountId in ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529'))
and (c.DP_SERVICELINE_KEY in ("A0A885C1-8A23-4218-A7A0-F7236ADBF4AD"))
and c.FacilityId in ("E4F65B8F-C501-47F8-A226-8E50E46EEAA2")
and c.warehouseCode in ("WCMH1")
and c.Designator in ('B_RMA')
and c.LPNNumber in ('632113563')
and c.VendorSerialNumber in ('NNTML21G33W3')
and c.VendorLotNumber in ('NNTML21G33W3')
and (
c.InvRef1 in ('REPLEN') 
or c.InvRef2 in ('REPLEN')  
or c.InvRef3  in ('REPLEN')  
or c.InvRef4  in ('REPLEN')  
or c.InvRef5  in ('REPLEN'))
and c.itemNumber = 'NT0H11BC' and c.is_deleted = 0
and (c.ExpirationDate between '2021-11-01 00:00:00.000' and '2021-11-11 00:00:00.000') -- null for all records
 ) a
where 
 a.batchStatus in (null) --null for given account id
and a.reasonCode in (null)  --null for given account id
group by 
a.warehouseId,      
a.warehouseCode,      
a.itemNumber,      
a.itemDescription,      
a.hazardClass,      
a.itemDimensions_length,      
a.itemDimensions_width,      
a.itemDimensions_height,      
a.itemDimensions_unitOfMeasurement,      
a.itemWeight_weight,      
a.itemWeight_unitOfMeasurement,      
a.SourceSystemKey,      
a.referenceNumber1,      
a.referenceNumber2,      
a.referenceNumber3,      
a.referenceNumber4,      
a.referenceNumber5,      
a.HazmatClass,      
a.StrategicGoodsFlag,      
a.UNCode,      
a.LPN,      
a.VSN,      
a.VCL,      
a.Designator,      
a.batchStatus,      
a.Account,      
a.expirationDate,      
a.batchNumber,      
a.inventoryReasonCode_nonAvailableQuantity,    
a.reasonCode 

-- Result Set 2

/*
Target Container - digital_summary_inventory
*/

select distinct
c.VendorLotNumber BatchNumber
from c
where 
(c.AccountId in ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529'))
and (c.DP_SERVICELINE_KEY in ("A0A885C1-8A23-4218-A7A0-F7236ADBF4AD"))
and c.FacilityId in ("E4F65B8F-C501-47F8-A226-8E50E46EEAA2")
and c.warehouseCode in ("WCMH1")
and c.Designator in ('B_RMA')
and c.LPNNumber in ('632113563')
and c.VendorSerialNumber in ('NNTML21G33W3')
and c.VendorLotNumber in ('NNTML21G33W3')
and (
c.InvRef1 in ('REPLEN') 
or c.InvRef2 in ('REPLEN')  
or c.InvRef3  in ('REPLEN')  
or c.InvRef4  in ('REPLEN')  
or c.InvRef5  in ('REPLEN'))
and c.itemNumber = 'NT0H11BC' and c.is_deleted = 0 
and (c.ExpirationDate between '2021-11-01 00:00:00.000' and '2021-11-11 00:00:00.000') -- null for all records

-- Result Set 3

/*
Target Container - digital_summary_inventory
*/

select 
a.batchStatus BatchStatus,
((a.reasonCode = 'NA'or a.reasonCode = null) ? '':a.reasonCode)  ReasonCode,
count(a.batchNumber) ItemQuantity
from
(select 
'Y'='Y'? c.VendorLotNumber : null batchNumber, --@isBatchInformationRequired='Y'
'Y'='Y'?c.BatchStatus : null batchStatus, --@isBatchInformationRequired='Y'
'Y'='Y'? (c.BatchStatus = 'Released'? 'NA':c.BatchHoldReason) : null reasonCode --@isBatchInformationRequired='Y'
from c
where 
(c.AccountId in ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529'))
and (c.DP_SERVICELINE_KEY in ("A0A885C1-8A23-4218-A7A0-F7236ADBF4AD"))
and c.FacilityId in ("E4F65B8F-C501-47F8-A226-8E50E46EEAA2")
and c.warehouseCode in ("WCMH1")
and c.Designator in ('B_RMA')
and c.LPNNumber in ('632113563')
and c.VendorSerialNumber in ('NNTML21G33W3')
and c.VendorLotNumber in ('NNTML21G33W3')
and (
c.InvRef1 in ('REPLEN') 
or c.InvRef2 in ('REPLEN')  
or c.InvRef3  in ('REPLEN')  
or c.InvRef4  in ('REPLEN')  
or c.InvRef5  in ('REPLEN'))
and c.itemNumber = 'NT0H11BC' and c.is_deleted = 0 
and (c.ExpirationDate between '2021-11-01 00:00:00.000' and '2021-11-11 00:00:00.000') -- null for all records
) a
group by a.batchStatus , a.reasonCode