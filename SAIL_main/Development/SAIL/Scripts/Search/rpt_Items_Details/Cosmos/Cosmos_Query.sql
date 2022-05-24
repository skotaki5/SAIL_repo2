-- rpt_Items_Details

-- Result Set 1

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
(c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId =@DPProductLineKey )
and (c.DP_SERVICELINE_KEY in (@AccountKeys.DPServiceLineKey) or c.DP_SERVICELINE_KEY =@DPServiceLineKey)
and c.FacilityId in (@warehouse.warehouseID)
and c.warehouseCode in (@warehouse.warehouseName)
and c.Designator in (@Designator)
and c.LPNNumber in (@LPN)
and c.VendorSerialNumber in (@VSN)
and c.VendorLotNumber in (@VCL)
and c.VendorLotNumber in (@batchNumber )
and (
c.InvRef1 in (@InboundReferenceNumber) 
or c.InvRef2 in (@InboundReferenceNumber)  
or c.InvRef3  in (@InboundReferenceNumber)  
or c.InvRef4  in (@InboundReferenceNumber)  
or c.InvRef5  in (@InboundReferenceNumber))
and c.itemNumber = @itemNumber and c.is_deleted = 0
and (c.ExpirationDate between @ExpirationDate.ExpirationStartDate and @ExpirationDate.ExpirationEndDate)) a
where 
 a.batchStatus in (@BatchStatus) --null for given account id
and a.reasonCode in (@ReasonCode) -- null for given account id
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

select distinct
c.VendorLotNumber BatchNumber
from c
where 
(c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId =@DPProductLineKey )
and (c.DP_SERVICELINE_KEY in (@AccountKeys.DPServiceLineKey) or c.DP_SERVICELINE_KEY =@DPServiceLineKey)
and c.FacilityId in (@warehouse.warehouseID)
and c.warehouseCode in (@warehouse.warehouseName)
and c.Designator in (@Designator)
and c.LPNNumber in (@LPN)
and c.VendorSerialNumber in (@VSN)
and c.VendorLotNumber in (@VCL)
and c.VendorLotNumber in (@batchNumber )
and (
c.InvRef1 in (@InboundReferenceNumber) 
or c.InvRef2 in (@InboundReferenceNumber)  
or c.InvRef3  in (@InboundReferenceNumber)  
or c.InvRef4  in (@InboundReferenceNumber)  
or c.InvRef5  in (@InboundReferenceNumber))
and c.itemNumber = @itemNumber and c.is_deleted = 0
and (c.ExpirationDate between @ExpirationDate.ExpirationStartDate and @ExpirationDate.ExpirationEndDate)

-- Result Set 3

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
(c.AccountId in (@AccountKeys.DPProductLineKey) or c.AccountId =@DPProductLineKey )
and (c.DP_SERVICELINE_KEY in (@AccountKeys.DPServiceLineKey) or c.DP_SERVICELINE_KEY =@DPServiceLineKey)
and c.FacilityId in (@warehouse.warehouseID)
and c.warehouseCode in (@warehouse.warehouseName)
and c.Designator in (@Designator)
and c.LPNNumber in (@LPN)
and c.VendorSerialNumber in (@VSN)
and c.VendorLotNumber in (@VCL)
and c.VendorLotNumber in (@batchNumber )
and (
c.InvRef1 in (@InboundReferenceNumber) 
or c.InvRef2 in (@InboundReferenceNumber)  
or c.InvRef3  in (@InboundReferenceNumber)  
or c.InvRef4  in (@InboundReferenceNumber)  
or c.InvRef5  in (@InboundReferenceNumber))
and c.itemNumber = @itemNumber and c.is_deleted = 0
and (c.ExpirationDate between @ExpirationDate.ExpirationStartDate and @ExpirationDate.ExpirationEndDate)) a

group by a.batchStatus , a.reasonCode