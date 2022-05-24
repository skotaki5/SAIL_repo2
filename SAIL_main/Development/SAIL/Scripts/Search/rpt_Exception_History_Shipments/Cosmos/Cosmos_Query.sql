-- rpt_Exception_History_Shipments

/*
NOTE:
completed /-- order by t.UTC_ExceptionCreatedDate desc --not supported need to do in be
*/

--Result Set 1

/*
Parameter Requirement info -
->@DPProductLineKey  required 
->@DPServiceLineKey  optional
->@UPSOrderNumber required

Target Container - digital_summary_orders
*/

--   IF @shipmentType= 'INBOUND' or @shipmentType= 'MOVEMENT'

select Distinct
(t.exceptionType = ''?null : t.exceptionType) ExceptionType,
t.OTZ_ExceptionCreatedDate creationDateTime, 
t.exceptionReason ,
t.ExceptionPrimaryIndicator PrimaryIndicator
from c
join t IN c.exception_list
where 
c.UPSOrderNumber = @UPSShipmentNumber AND c.is_deleted=0
and (c.IS_INBOUND =1 or c.IS_INBOUND =2 )
-- order by t.UTC_ExceptionCreatedDate desc --not supported need to do in be

--   IF @shipmentType= 'OUTBOUND'

select Distinct
(t.exceptionType = ''?null : t.exceptionType) ExceptionType,
t.OTZ_ExceptionCreatedDate creationDateTime,
t.exceptionReason ,
t.ExceptionPrimaryIndicator PrimaryIndicator
from c
join t in c.exception_list
where 
c.AccountId = @DPProductLineKey and c.is_deleted=0
AND c.DP_SERVICELINE_KEY = @DPServiceLineKey 
and c.UPSOrderNumber = @UPSShipmentNumber
and c.IS_INBOUND =0
-- order by t.UTC_ExceptionCreatedDate desc --not supported need to do in be