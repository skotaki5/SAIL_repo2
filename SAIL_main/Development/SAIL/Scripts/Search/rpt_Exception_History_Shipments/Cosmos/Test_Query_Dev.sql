-- rpt_Exception_History_Shipments

--Result Set 1

/*
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
c.UPSOrderNumber = '43508142' 
and c.is_deleted=0
and (c.IS_INBOUND =1 or c.IS_INBOUND =2 )

--   IF @shipmentType= 'OUTBOUND'

select Distinct
(t.exceptionType = ''?null : t.exceptionType) ExceptionType,
t.OTZ_ExceptionCreatedDate creationDateTime,
t.exceptionReason ,
t.ExceptionPrimaryIndicator PrimaryIndicator
from c
join t in c.exception_list
where 
c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
AND c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD' 
and c.UPSOrderNumber = '85753969' and c.is_deleted=0
and c.IS_INBOUND =0
-- order by t.UTC_ExceptionCreatedDate desc --not supported need to do in be