-- rpt_Inbound_Activity_Line_Volume_DayLevel_Summary

--Result Set 1

/*
Parameter Requirement info -
----> @startDate, @endDate and @DPProductLineKey are required,
@warehouseId optional

 Target Container - digital_summary_milestone_activity
*/

SELECT COUNT(1) AS Total from c
WHERE (c.ActivityDate between @startDate and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@endDate)))
and c.AccountId = @DPProductLineKey
AND c.FacilityId in (@warehouseId) and c.is_deleted=0 and c.ActivityCode IN ( 'RECS','REC30','REC90')

 --Result Set 2

/*
Parameter Requirement info -
----> @startDate, @endDate and @DPProductLineKey are required,
@warehouseId optional

 Target Container - digital_summary_milestone_activity
*/
Select a.ReceivingDate, a.LINES from (SELECT MAX(c._rid) _rid,c.ActivityDateShort ReceivingDate,COUNT(1) as LINES FROM c -- (workaround) c._rid is added as it is required by the outer order by on the subquery so that it works with comos .net sdk (ohterwise it is not required on cosmos editor)
WHERE (c.ActivityDate between @startDate and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1,@endDate)))
and c.AccountId = @DPProductLineKey
AND c.FacilityId in (@warehouseId) 
and c.is_deleted=0 c.ActivityCode IN ( 'RECS','REC30','REC90')
 GROUP BY c.ActivityDateShort)a ORDER BY a.ReceivingDate
