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
(is_null(c.DateTimeReceived)?true:c.DateTimeReceived between @__startDateTime_0 and @__endDateTime_1)
and
 (c.AccountId != null and c.AccountId in (@__ToUpper_2))
and (c.FacilityId != null and c.FacilityId in (@__ToUpper_3))
-- and (c.DP_SERVICELINE_KEY != null and c.DP_SERVICELINE_KEY in ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'))
and c.ActualDeliveryDate > c.actualScheduledDeliveryDateTime  
and c.OrderCancelledFlag = "N"
and c.IS_INBOUND = 0
and t.ExceptionPrimaryIndicator = '1'
and (t.ExceptionCategory != null and t.ExceptionCategory != "Save")
and c.is_deleted =0
group by t.exceptionReason,
t.ExceptionReasonType