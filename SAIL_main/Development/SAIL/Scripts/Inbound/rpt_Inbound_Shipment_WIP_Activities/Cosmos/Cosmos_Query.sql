--AUTHOR : SHREY JAIN
--DESCRIPTION:rpt_Inbound_Shipment_WIP_Activities
--DATE :29-04-2022

/*NOTE:
       @type : '*', 'ASN', 'LINES', 'UNITS', 'CASES'



  CASE 1 :

  IF @startDate IS NULL OR @endDate IS NULL    
 BEGIN    
    SET @NULLDate = '*'    
 END    
 

CASE 2:


 IF UPPER(@dateType)  = 'SHIPMENTCREATIONDATE'    
    SET @NULLReceivedDate = '*'    

CASE 3:    
  IF UPPER(@dateType)  = 'SHIPMENTRECEIVEDDATE'    
    SET @NULLCreatedDate = '*'    



*/

--Result Set 1

/*
-- Parameter requirement info.
@DPProductLineKey required
@DPServiceLineKey optional
@AccountKeys      required
@warehouseId      optional
@startDate        optional
@endDate          optional
@dateType         optional
@type             required

--Target container digital_summary_orders
*/

-- condition 1:  IF UPPER(@dateType)  = 'SHIPMENTCREATIONDATE' or UPPER(@dateType)  = 'SHIPMENTRECEIVEDDATE'
 
SELECT a.type
    ,a.ActivityName
    ,a.warehouseCode
    ,a.Count = null ? 0 : a.Count Count
    ,a.Date
    ,a.ActivityOrderId
FROM (
    SELECT UPPER(@type) type
        ,b.ActivityOrderId
        ,b.ActivityName
        ,b.warehouseCode
        ,b.Date
        ,(UPPER(@type) = 'ASN' ? Count(1) : (UPPER(@type) = 'LINES' ? sum(b.inbound_line_count) : (UPPER(@type) = 'UNITS' ? sum(b.inbound_ShippedQuantity_sum) : (UPPER(@type) = 'CASES' ? sum(b.inbound_cases_sum) :null)))) Count
    FROM (
        SELECT t.WIPActivityOrderId ActivityOrderId
            ,t.WIP_ActivityName ActivityName
            ,c.warehouseCode
            , (@NULLDate = '*' AND @NULLReceivedDate = '*') ? c.ma_ActivityDate_part_1 : (@NULLCreatedDate = '*' ? c.ma_ActivityDate_part_2 : (@NULLReceivedDate = '*' ? c.ShipmentCreationDate: null)) Date
            ,c.inbound_line_count != null ? c.inbound_line_count : 0 inbound_line_count
            ,c.inbound_ShippedQuantity_sum != null ? c.inbound_ShippedQuantity_sum : 0 inbound_ShippedQuantity_sum
            ,c.inbound_cases_sum != null ? c.inbound_cases_sum : 0 inbound_cases_sum
        FROM c
        JOIN t IN c.WIP_ActivityName
        WHERE (c.AccountId IN (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
            AND (c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey) or c.DP_SERVICELINE_KEY = @DPServiceLineKey)
            AND c.FacilityId IN (@warehouseId)
            AND (
                (
                    c.DateTimeReceived BETWEEN @startDate
                        AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
                    )
                OR @NULLCreatedDate = '*' OR @NULLDate = '*'
                )
            AND (
                (
                    c.ma_ActivityDate_2 BETWEEN @startDate
                        AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
                    )
                OR @NULLReceivedDate = '*' OR @NULLDate = '*'
                )
            AND ((@NULLReceivedDate = '*') ? c.ma_ActivityDate_1 !=null : ((@NULLCreatedDate = '*') ? c.ma_ActivityDate_2 !=null : true) )
            AND c.ShipmentCreationDate > DateTimeAdd("dd", -90, GetCurrentDateTime())
            AND c.IS_INBOUND = 1
            AND c.IS_ASN = 1
            AND t.Type = 'IN'
            AND c.is_deleted = 0
        ) b
    GROUP BY b.ActivityOrderId
        ,b.ActivityName
        ,b.warehouseCode
        ,b.Date
    ) a
--    order by a.ActivityOrderId  to be done in backend
--  if UPPER(@type) = '*'
 --  aggregate result for all cases 'ASN', 'LINES', 'UNITS', 'CASES'
 
-- condition 2:  IF UPPER(@dateType)  = '' or  UPPER(@dateType)  = '*' or UPPER(@dateType)  = null
 
 
SELECT a.type
    ,a.ActivityName
    ,a.warehouseCode
    ,a.Count = null ? 0 : a.Count Count
    ,a.Date
    ,a.ActivityOrderId
FROM (
    SELECT UPPER(@type) type
        ,b.ActivityOrderId
        ,b.ActivityName
        ,b.warehouseCode
        ,b.Date
        ,(UPPER(@type) = 'ASN' ? Count(1) : (UPPER(@type) = 'LINES' ? sum(b.inbound_line_count) : (UPPER(@type) = 'UNITS' ? sum(b.inbound_ShippedQuantity_sum) : (UPPER(@type) = 'CASES' ? sum(b.inbound_cases_sum) :null)))) Count
    FROM (
        SELECT t.WIPActivityOrderId ActivityOrderId
            ,t.WIP_ActivityName ActivityName
            ,c.warehouseCode
            ,@NULLDate = '*' ? d : null Date
            ,c.inbound_line_count != null ? c.inbound_line_count : 0 inbound_line_count
            ,c.inbound_ShippedQuantity_sum != null ? c.inbound_ShippedQuantity_sum : 0 inbound_ShippedQuantity_sum
            ,c.inbound_cases_sum != null ? c.inbound_cases_sum : 0 inbound_cases_sum
        FROM c
        JOIN t IN c.WIP_ActivityName
        JOIN d IN c.ma_ActivityDate_list
        WHERE (c.AccountId IN (@AccountKeys.DPProductLineKey) or c.AccountId = @DPProductLineKey)
            AND (c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey) or c.DP_SERVICELINE_KEY = @DPServiceLineKey)
            AND c.FacilityId IN (@warehouseId)
            AND (
                (
                    c.DateTimeReceived BETWEEN @startDate
                        AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, @endDate))
                    )
                OR @NULLCreatedDate = '*' OR @NULLDate = '*'
                )
            AND (
                (
                    d BETWEEN @startDate
                        AND @endDate
                    )
                OR @NULLReceivedDate = '*' OR @NULLDate = '*'
                )
            AND c.ShipmentCreationDate > DateTimeAdd("dd", -90, GetCurrentDateTime())
            AND d != ''  
            AND c.IS_INBOUND = 1
            AND c.IS_ASN = 1
            AND t.Type = 'IN'
            AND c.is_deleted = 0
        ) b
    GROUP BY b.ActivityOrderId
        ,b.ActivityName
        ,b.warehouseCode
        ,b.Date
    ) a
 
--    order by a.ActivityOrderId  to be done in backend
--  if UPPER(@type) = '*'
 --  aggregate result for all cases 'ASN', 'LINES', 'UNITS', 'CASES'