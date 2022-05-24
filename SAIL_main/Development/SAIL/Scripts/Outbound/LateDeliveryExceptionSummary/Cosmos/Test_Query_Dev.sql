--LateDeliveryExceptionSummary

--Result Set 1

/*
Target Container - digital_summary_orders
*/

select t.exceptionReason LateDeliveryExceptionReason, 
count(1) Count,
t.ExceptionReasonType ReasonType
from c
join t in c.exception_list
where 
(is_null(c.DateTimeReceived)?true:c.DateTimeReceived between '2022-02-01 00:00:00.000' and '2022-02-07 00:00:00.000')
and
 (c.AccountId != null and c.AccountId in ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529' ))
and (c.FacilityId != null and c.FacilityId in ('A2B979C6-7FA9-40DE-A648-08D6175D2695'))
-- and (c.DP_SERVICELINE_KEY != null and c.DP_SERVICELINE_KEY in ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'))
and c.ActualDeliveryDate > c.actualScheduledDeliveryDateTime  
and c.OrderCancelledFlag = "N"
and c.IS_INBOUND = 0
and t.ExceptionPrimaryIndicator = '1'
and (t.ExceptionCategory != null and t.ExceptionCategory != "Save")
and c.is_deleted =0
group by t.exceptionReason,
t.ExceptionReasonType