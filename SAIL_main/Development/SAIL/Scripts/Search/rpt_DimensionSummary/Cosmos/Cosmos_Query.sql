--rpt_DimensionSummary

/*
NOTE;
IS_INBOUND is a numeric field and has to be mapped like: 
ShipmentType='INBOUND' THEN 1
ShipmentType='MOVEMENT' THEN 2
ShipmentType='OUTBOUND' THEN 0 

complete / add order by for originCity and destination city
*/

--Result Set 1

/*
Parameter Requirement info -
->@AccountKeys required
"->@DPProductLineKey  required 
->@DPServiceLineKey  optional
->@Date.shipmentCreationStartDate      optional
->@Date.shipmentCreationEndDate        optional
->@ShipmentType optional
->@ResultsetType optional


Target Container - digital_summary_orders
*/

--   IF  @ResultsetType='ORIGINSUMMARY'
 
SELECT DISTINCT trim(upper(c.OriginCity)) AS OriginCity
    ,trim(upper(c.OriginCountry)) AS OriginCountry
FROM c
WHERE (
        c.AccountId IN (@AccountKeys.DPProductLineKey)
        OR c.AccountId =@DPProductLineKey
        )
    AND (
        c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
        OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
        )
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
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
        @ShipmentType
        ,c.IS_INBOUND = 0 ? 'OUTBOUND' : (c.IS_INBOUND = 1 ? 'INBOUND' : (c.IS_INBOUND = 2 ? 'MOVEMENT' : null))
        ,true
        )
-- ORDER BY OriginCountry --to be applied in BE
 
--    IF  @ResultsetType='DESTINATIONSUMMARY'
 
SELECT DISTINCT Trim(UPPER(c.DestinationCity)) DestinationCity
    ,Trim(UPPER(c.DestinationCountry)) DestinationCountry
FROM c
WHERE (
        c.AccountId IN (@AccountKeys.DPProductLineKey)
        OR c.AccountId =@DPProductLineKey
        )
    AND (
        c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
        OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
        )
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
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
        @ShipmentType
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
        c.AccountId IN (@AccountKeys.DPProductLineKey)
        OR c.AccountId =@DPProductLineKey
        )
    AND (
        c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
        OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
        )
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND c.is_deleted = 0
    AND CONTAINS (
        @ShipmentType
        ,c.IS_INBOUND = 0 ? 'OUTBOUND' : (c.IS_INBOUND = 1 ? 'INBOUND' : (c.IS_INBOUND = 2 ? 'MOVEMENT' : null))
        ,true
        )
ORDER BY c.shipmentDescription
 
 
--  IF  @ResultsetType = '*' or @ResultsetType = '' or @ResultsetType = null
 
SELECT DISTINCT trim(upper(c.OriginCity)) AS OriginCity
    ,trim(upper(c.OriginCountry)) AS OriginCountry
FROM c
WHERE (
        c.AccountId IN (@AccountKeys.DPProductLineKey)
        OR c.AccountId =@DPProductLineKey
        )
    AND (
        c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
        OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
        )
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
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
        @ShipmentType
        ,c.IS_INBOUND = 0 ? 'OUTBOUND' : (c.IS_INBOUND = 1 ? 'INBOUND' : (c.IS_INBOUND = 2 ? 'MOVEMENT' : null))
        ,true
        )
-- ORDER BY OriginCountry --to be applied in BE
 
SELECT DISTINCT Trim(UPPER(c.DestinationCity)) DestinationCity
    ,Trim(UPPER(c.DestinationCountry)) DestinationCountry
FROM c
WHERE (
        c.AccountId IN (@AccountKeys.DPProductLineKey)
        OR c.AccountId =@DPProductLineKey
        )
    AND (
        c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
        OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
        )
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
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
        @ShipmentType
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
        c.AccountId IN (@AccountKeys.DPProductLineKey)
        OR c.AccountId =@DPProductLineKey
        )
    AND (
        c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey)
        OR c.DP_SERVICELINE_KEY = @DPServiceLineKey
        )
    AND (
        c.DateTimeReceived BETWEEN @Date.shipmentCreationStartDate
            AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @Date.shipmentCreationEndDate))
        )
    AND c.is_deleted = 0
    AND CONTAINS (
        @ShipmentType
        ,c.IS_INBOUND = 0 ? 'OUTBOUND' : (c.IS_INBOUND = 1 ? 'INBOUND' : (c.IS_INBOUND = 2 ? 'MOVEMENT' : null))
        ,true
        )
ORDER BY c.shipmentDescription