--AUTHOR : 		VISHAL SHARMA
--DESCRIPTION:	rpt_Outbound_Shipment_DayLevel_Summary
--DATE : 		05-04-2022

Result Set 1
CASE 1:   IF @DateType = 'shipmentCreationDate'
--* Target Container-digital_summary_orders
 
SELECT count(1) total from T
WHERE
		(T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529') --Sprint 52 Changes
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD') --Sprint 52 Changes
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
-- 		AND (T.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --IF @DateType = 'shipmentShippedDate'
		AND (T.DateTimeReceived BETWEEN '2022-01-10 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000')))  --  IF @DateType = 'shipmentCreationDate'
-- 		AND ((T.actualDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000')))) -- IF @DateType = 'actualDeliveryDate'
-- for else condition all date filters will be applied
		AND T.IS_INBOUND = 0 
		AND T.is_deleted=0
		AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed --Sprint 52 Changes
--		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'--Sprint 52 Changes
 
CASE 2:  IF @DateType = 'shipmentShippedDate'
--* Target Container-digital_summary_orders 

SELECT count(1) total from T
WHERE
		(T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529') --Sprint 52 Changes
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD') 
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')--Sprint 52 Changes
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
		AND (T.DateTimeShipped BETWEEN '2022-01-10 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --IF @DateType = 'shipmentShippedDate'
--		AND (T.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000')))  --IF @DateType = 'shipmentCreationDate'
-- 		AND ((T.actualDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000')))) -- IF @DateType = 'actualDeliveryDate'
-- for else condition all date filters will be applied
		AND T.IS_INBOUND = 0 
		AND T.is_deleted=0
		AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed --Sprint 52 Changes
--		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'--Sprint 52 Changes
		
CASE 3:  IF @DateType = 'actualDeliveryDate'
--* Target Container-digital_summary_orders

SELECT count(1) total from T
WHERE
		(T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')--Sprint 52 Changes
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')--Sprint 52 Changes
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
--		AND (T.DateTimeShipped BETWEEN '2022-01-01 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --IF @DateType = 'shipmentShippedDate'
--		AND (T.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000')))  --  IF @DateType = 'shipmentCreationDate'
		AND ((T.actualDeliveryDateTime BETWEEN '2022-01-10 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000')))) -- IF @DateType = 'actualDeliveryDate'
-- for else condition all date filters will be applied
		AND T.IS_INBOUND = 0 
		AND T.is_deleted=0
		AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed --Sprint 52 Changes
--		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B'))--Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'	--Sprint 52 Changes	
		
CASE 4:  ELSE
--* Target Container-digital_summary_orders

SELECT count(1) total from T
WHERE
		(T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')--Sprint 52 Changes
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')--Sprint 52 Changes
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
		AND (T.DateTimeShipped BETWEEN '2022-01-10 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --IF @DateType = 'shipmentShippedDate'
		AND (T.DateTimeReceived BETWEEN '2022-01-10 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000')))  --  IF @DateType = 'shipmentCreationDate'
		AND ((T.actualDeliveryDateTime BETWEEN '2022-01-10 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000')))) -- IF @DateType = 'actualDeliveryDate'
-- for else condition all date filters will be applied
		AND T.IS_INBOUND = 0 
		AND T.is_deleted=0
		AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed--Sprint 52 Changes
--		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B'))--Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'	--Sprint 52 Changes
		 
Result Set 2 IF @DateType = 'shipmentCreationDate'

SELECT
      T.ShipmentCreationDate ShipmentCreationDate,
      COUNT(1) AS ShipmentCreationDateCount
    FROM T 
    WHERE 
		is_null(T.ShipmentCreationDate) = false
		AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')--Sprint 52 Changes
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')--Sprint 52 Changes
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
		AND (T.DateTimeReceived BETWEEN '2022-01-10 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000')))  --  IF @DateType = 'shipmentCreationDate'
		AND T.IS_INBOUND = 0
		AND T.is_deleted = 0	
    	AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed--Sprint 52 Changes
--		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B'))--Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'--Sprint 52 Changes
	GROUP BY T.ShipmentCreationDate
--  ORDER BY T.ShipmentCreationDate 
--  Order by to be applied at BACKEND		
 
Result Set 3: IF @DateType = 'shipmentShippedDate'

SELECT
      T.ShipmentShippedDate ShipmentShippedDate,
      COUNT(1) AS ShipmentShippedDateCount
    FROM T 
    WHERE 
		is_null(T.ShipmentShippedDate) = false
		AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')--Sprint 52 Changes
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')--Sprint 52 Changes
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
		AND (T.DateTimeShipped BETWEEN '2022-01-10 00:00:00.000' AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000'))) --IF @DateType = 'shipmentShippedDate'
		AND T.IS_INBOUND = 0
		AND T.is_deleted = 0	
    	AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed--Sprint 52 Changes
--		AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B'))--Sprint 52 Changes
		AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'--Sprint 52 Changes
	GROUP BY T.ShipmentShippedDate
--  ORDER BY T.DateTimeShipped
--  Order by to be applied at BACKEND

Result Set 4	IF @DateType = 'actualDeliveryDate' --Sprint 52 Changes

SELECT
      T.ActualDeliveryDateTime_date AS ActualDeliveryDateTime, 
      COUNT(1) AS ActualDeliveryDateCount
    FROM T 
    WHERE 
	is_null(T.ActualDeliveryDateTime_date) = false
   	AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
    AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
    AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
	AND ((T.actualDeliveryDateTime BETWEEN '2022-01-01 00:00:00.000' 
    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-28 00:00:00.000')))) -- IF @DateType = 'actualDeliveryDate'
	AND T.IS_INBOUND = 0
    AND T.is_deleted = 0	
    AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
--	AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B'))
	AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) = 'N'		
GROUP BY T.ActualDeliveryDateTime_date--Sprint 52 Changes
--    ORDER BY T.actualDeliveryDateTime
-- 	  Order by to be applied at BACKEND