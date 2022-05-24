-- rpt_Inbound_Activity_Line_Volume_DayLevel_Summary

--Result Set 1

/*
 Target Container - digital_summary_milestone_activity
*/

SELECT COUNT(1) AS Total from c
WHERE (c.ActivityDate between '2021-12-16 00:00:00.000' 
and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2021-12-31 00:00:00.000')))
and c.AccountId = "1EEF1B1A-A415-43F3-88C5-2D5EBC503529"
AND c.FacilityId in ("EB377718-DAB0-4491-A65C-08D6175D2695")
 and c.is_deleted=0 and c.ActivityCode IN ( 'RECS','REC30','REC90')

--Result Set 2

/*
 Target Container - digital_summary_milestone_activity
*/

Select a.ReceivingDate, a.LINES from (SELECT MAX(c._rid) _rid,c.ActivityDateShort ReceivingDate,COUNT(1) as LINES FROM c -- (workaround) c._rid is added as it is required by the outer order by on the subquery so that it works with comos .net sdk (ohterwise it is not required on cosmos editor)
WHERE (c.ActivityDate between '2021-12-16 00:00:00.000' and DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, '2022-01-01 00:00:00.000')))
and c.AccountId = "1EEF1B1A-A415-43F3-88C5-2D5EBC503529"
AND c.FacilityId in ("EB377718-DAB0-4491-A65C-08D6175D2695") 
and c.is_deleted=0 and c.ActivityCode IN ( 'RECS','REC30','REC90')
 GROUP BY c.ActivityDateShort)a ORDER BY a.ReceivingDate
