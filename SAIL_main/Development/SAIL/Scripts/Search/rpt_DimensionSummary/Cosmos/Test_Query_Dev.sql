--rpt_DimensionSummary

--Result Set 1

/*
Target Container - digital_summary_orders
*/

--   IF  @ResultsetType='ORIGINSUMMARY'
 
SELECT DISTINCT trim(upper(c.OriginCity)) AS OriginCity
    ,trim(upper(c.OriginCountry)) AS OriginCountry
FROM c
WHERE (
        c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
        )
    AND (
        c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
        )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-05 00:00:00.000'))
        )
    AND c.OriginCity != null
    AND NOT CONTAINS (
        c.OriginCity
        ,'NOT AVAILABLE'
        ,true
        )
    AND c.OriginCountry != null
    AND NOT CONTAINS (
        c.OriginCountry
        ,'NOT AVAILABLE'
        ,true
        )
    AND c.is_deleted = 0
    AND CONTAINS (
        'outbound,inbound'
        ,c.IS_INBOUND = 0 ? 'OUTBOUND' : (c.IS_INBOUND = 1 ? 'INBOUND' : (c.IS_INBOUND = 2 ? 'MOVEMENT' : null))
        ,true
        )
-- ORDER BY OriginCountry --to be applied in BE
 
--    IF  @ResultsetType='DESTINATIONSUMMARY'
 
SELECT DISTINCT Trim(UPPER(c.DestinationCity)) DestinationCity
    ,Trim(UPPER(c.DestinationCountry)) DestinationCountry
FROM c
WHERE (
        c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
        )
    AND (
        c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
        )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-05 00:00:00.000'))
        )
    AND c.DestinationCity != null
    AND NOT CONTAINS (
        c.DestinationCity
        ,'NOT AVAILABLE'
        ,true
        )
    AND c.DestinationCountry != null
    AND NOT CONTAINS (
        c.DestinationCountry
        ,'NOT AVAILABLE'
        ,true
        )
    AND c.is_deleted = 0
    AND CONTAINS (
        'outbound,inbound'
        ,c.IS_INBOUND = 0 ? 'OUTBOUND' : (c.IS_INBOUND = 1 ? 'INBOUND' : (c.IS_INBOUND = 2 ? 'MOVEMENT' : null))
        ,true
        )
--ORDER BY DestinationCountry -- to be applied in BE
 
--   IF  @ResultsetType='ORDERTYPESUMMARY'
 
SELECT DISTINCT (
        c.shipmentDescription = ''
        OR c.shipmentDescription = null
        ) ? null : UPPER(c.shipmentDescription) OrderType
FROM c
WHERE (
        c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
        )
    AND (
        c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
        )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-05 00:00:00.000'))
        )
    AND c.is_deleted = 0
    AND CONTAINS (
        'outbound,inbound'
        ,c.IS_INBOUND = 0 ? 'OUTBOUND' : (c.IS_INBOUND = 1 ? 'INBOUND' : (c.IS_INBOUND = 2 ? 'MOVEMENT' : null))
        ,true
        )
ORDER BY c.shipmentDescription
 
 
--  IF  @ResultsetType = '*' or @ResultsetType = '' or @ResultsetType = null
 
SELECT DISTINCT trim(upper(c.OriginCity)) AS OriginCity
    ,trim(upper(c.OriginCountry)) AS OriginCountry
FROM c
WHERE (
        c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
        )
    AND (
        c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
        )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-05 00:00:00.000'))
        )
    AND c.OriginCity != null
    AND NOT CONTAINS (
        c.OriginCity
        ,'NOT AVAILABLE'
        ,true
        )
    AND c.OriginCountry != null
    AND NOT CONTAINS (
        c.OriginCountry
        ,'NOT AVAILABLE'
        ,true
        )
    AND c.is_deleted = 0
    AND CONTAINS (
        'outbound,inbound'
        ,c.IS_INBOUND = 0 ? 'OUTBOUND' : (c.IS_INBOUND = 1 ? 'INBOUND' : (c.IS_INBOUND = 2 ? 'MOVEMENT' : null))
        ,true
        )
-- ORDER BY OriginCountry --to be applied in BE
 
SELECT DISTINCT Trim(UPPER(c.DestinationCity)) DestinationCity
    ,Trim(UPPER(c.DestinationCountry)) DestinationCountry
FROM c
WHERE (
        c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
        )
    AND (
        c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
        )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-05 00:00:00.000'))
        )
    AND c.DestinationCity != null
    AND NOT CONTAINS (
        c.DestinationCity
        ,'NOT AVAILABLE'
        ,true
        )
    AND c.DestinationCountry != null
    AND NOT CONTAINS (
        c.DestinationCountry
        ,'NOT AVAILABLE'
        ,true
        )
    AND c.is_deleted = 0
    AND CONTAINS (
        'outbound,inbound'
        ,c.IS_INBOUND = 0 ? 'OUTBOUND' : (c.IS_INBOUND = 1 ? 'INBOUND' : (c.IS_INBOUND = 2 ? 'MOVEMENT' : null))
        ,true
        )
--ORDER BY DestinationCountry -- to be applied in BE
 
SELECT DISTINCT (
        c.shipmentDescription = ''
        OR c.shipmentDescription = null
        ) ? null : UPPER(c.shipmentDescription) OrderType
FROM c
WHERE (
        c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        OR c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
        )
    AND (
        c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        OR c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
        )
    AND (
        c.DateTimeReceived BETWEEN '2022-01-01 00:00:00.000'
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-01-05 00:00:00.000'))
        )
    AND c.is_deleted = 0
    AND CONTAINS (
        'outbound,inbound'
        ,c.IS_INBOUND = 0 ? 'OUTBOUND' : (c.IS_INBOUND = 1 ? 'INBOUND' : (c.IS_INBOUND = 2 ? 'MOVEMENT' : null))
        ,true
        )
ORDER BY c.shipmentDescription