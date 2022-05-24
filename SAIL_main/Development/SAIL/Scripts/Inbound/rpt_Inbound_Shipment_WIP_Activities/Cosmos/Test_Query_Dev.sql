--AUTHOR : SHREY JAIN
--DESCRIPTION:rpt_Inbound_Shipment_WIP_Activities
--DATE :29-04-2022

--Result Set 1

--* Target Container-digital_summary_orders

-- condition 1:  IF UPPER(@dateType)  = 'SHIPMENTCREATIONDATE' or UPPER(@dateType)  = 'SHIPMENTRECEIVEDDATE'
 
--IF  UPPER(@type)='ASN'
 
SELECT a.type
 ,a.ActivityName
 ,a.warehouseCode
 ,a.Count = null ? 0 : a.Count Count
 ,a.Date
 ,a.ActivityOrderId
FROM (
 SELECT 'ASN' type
  ,b.ActivityOrderId
  ,b.ActivityName
  ,b.warehouseCode
  ,b.Date
  ,('ASN' = 'ASN' ? Count(1) : ('ASN' = 'LINES' ? sum(b.inbound_line_count) : ('ASN' = 'UNITS' ? sum(b.inbound_ShippedQuantity_sum) : ('ASN' = 'CASES' ? sum(b.inbound_cases_sum) :null)))) Count
 FROM (
  SELECT t.WIPActivityOrderId ActivityOrderId
   ,t.WIP_ActivityName ActivityName
   ,c.warehouseCode
   ,('' = '*' AND '*' = '*') ? c.ma_ActivityDate_part_1 : ('' = '*' ? c.ma_ActivityDate_part_2 : ('*' = '*' ? c.ShipmentCreationDate: null)) Date
   ,c.inbound_line_count != null ? c.inbound_line_count : 0 inbound_line_count
   ,c.inbound_ShippedQuantity_sum != null ? c.inbound_ShippedQuantity_sum : 0 inbound_ShippedQuantity_sum
   ,c.inbound_cases_sum != null ? c.inbound_cases_sum : 0 inbound_cases_sum
  FROM c
  JOIN t IN c.WIP_ActivityName
  WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
   AND c.DP_SERVICELINE_KEY IN ('53D60776-D3BD-4E6A-8E37-F591C148294B')
   -- AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
   AND (
    (
     c.DateTimeReceived BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '' = '*' OR '' = '*'
    )
   AND (
    (
     c.ma_ActivityDate_2 BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '*' = '*' OR '' = '*'
    )
   AND (('*'='*') ? c.ma_ActivityDate_1 !=null : ((''='*') ? c.ma_ActivityDate_2 !=null : true) )
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
 
--IF  UPPER(@type)='LINES'   
 
 SELECT a.type
 ,a.ActivityName
 ,a.warehouseCode
 ,a.Count = null ? 0 : a.Count Count
 ,a.Date
 ,a.ActivityOrderId
FROM (
 SELECT 'LINES' type
  ,b.ActivityOrderId
  ,b.ActivityName
  ,b.warehouseCode
  ,b.Date
  ,('LINES' = 'ASN' ? Count(1) : ('LINES' = 'LINES' ? sum(b.inbound_line_count) : ('LINES' = 'UNITS' ? sum(b.inbound_ShippedQuantity_sum) : ('LINES' = 'CASES' ? sum(b.inbound_cases_sum) :null)))) Count
 FROM (
  SELECT t.WIPActivityOrderId ActivityOrderId
   ,t.WIP_ActivityName ActivityName
   ,c.warehouseCode
   ,('' = '*' AND '*' = '*') ? c.ma_ActivityDate_part_1 : ('' = '*' ? c.ma_ActivityDate_part_2 : ('*' = '*' ? c.ShipmentCreationDate: null)) Date
   ,c.inbound_line_count != null ? c.inbound_line_count : 0 inbound_line_count
   ,c.inbound_ShippedQuantity_sum != null ? c.inbound_ShippedQuantity_sum : 0 inbound_ShippedQuantity_sum
   ,c.inbound_cases_sum != null ? c.inbound_cases_sum : 0 inbound_cases_sum
  FROM c
  JOIN t IN c.WIP_ActivityName
  WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
   AND c.DP_SERVICELINE_KEY IN ('53D60776-D3BD-4E6A-8E37-F591C148294B')
   -- AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
   AND (
    (
     c.DateTimeReceived BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '' = '*' OR '' = '*'
    )
   AND (
    (
     c.ma_ActivityDate_2 BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '*' = '*' OR '' = '*'
    )
   AND (('*'='*') ? c.ma_ActivityDate_1 !=null : ((''='*') ? c.ma_ActivityDate_2 !=null : true) )
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
 
 --  IF UPPER(@type)='UNITS'  
 
SELECT a.type
 ,a.ActivityName
 ,a.warehouseCode
 ,a.Count = null ? 0 : a.Count Count
 ,a.Date
 ,a.ActivityOrderId
FROM (
 SELECT 'UNITS' type
  ,b.ActivityOrderId
  ,b.ActivityName
  ,b.warehouseCode
  ,b.Date
  ,('UNITS' = 'ASN' ? Count(1) : ('UNITS' = 'LINES' ? sum(b.inbound_line_count) : ('UNITS' = 'UNITS' ? sum(b.inbound_ShippedQuantity_sum) : ('UNITS' = 'CASES' ? sum(b.inbound_cases_sum) :null)))) Count
 FROM (
  SELECT t.WIPActivityOrderId ActivityOrderId
   ,t.WIP_ActivityName ActivityName
   ,c.warehouseCode
   ,('' = '*' AND '*' = '*') ? c.ma_ActivityDate_part_1 : ('' = '*' ? c.ma_ActivityDate_part_2 : ('*' = '*' ? c.ShipmentCreationDate: null)) Date
   ,c.inbound_line_count != null ? c.inbound_line_count : 0 inbound_line_count
   ,c.inbound_ShippedQuantity_sum != null ? c.inbound_ShippedQuantity_sum : 0 inbound_ShippedQuantity_sum
   ,c.inbound_cases_sum != null ? c.inbound_cases_sum : 0 inbound_cases_sum
  FROM c
  JOIN t IN c.WIP_ActivityName
  WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
   AND c.DP_SERVICELINE_KEY IN ('53D60776-D3BD-4E6A-8E37-F591C148294B')
   -- AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
   AND (
    (
     c.DateTimeReceived BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '' = '*' OR '' = '*'
    )
   AND (
    (
     c.ma_ActivityDate_2 BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '*' = '*' OR '' = '*'
    )
   AND (('*'='*') ? c.ma_ActivityDate_1 !=null : ((''='*') ? c.ma_ActivityDate_2 !=null : true) )
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
 
--   IF UPPER(@type)='*' OR UPPER(@type)='CASES'
 
SELECT a.type
 ,a.ActivityName
 ,a.warehouseCode
 ,a.Count = null ? 0 : a.Count Count
 ,a.Date
 ,a.ActivityOrderId
FROM (
 SELECT 'CASES' type
  ,b.ActivityOrderId
  ,b.ActivityName
  ,b.warehouseCode
  ,b.Date
  ,('CASES' = 'ASN' ? Count(1) : ('CASES' = 'LINES' ? sum(b.inbound_line_count) : ('CASES' = 'UNITS' ? sum(b.inbound_ShippedQuantity_sum) : ('CASES' = 'CASES' ? sum(b.inbound_cases_sum) :null)))) Count
 FROM (
  SELECT t.WIPActivityOrderId ActivityOrderId
   ,t.WIP_ActivityName ActivityName
   ,c.warehouseCode
   ,('' = '*' AND '*' = '*') ? c.ma_ActivityDate_part_1 : ('' = '*' ? c.ma_ActivityDate_part_2 : ('*' = '*' ? c.ShipmentCreationDate: null)) Date
   ,c.inbound_line_count != null ? c.inbound_line_count : 0 inbound_line_count
   ,c.inbound_ShippedQuantity_sum != null ? c.inbound_ShippedQuantity_sum : 0 inbound_ShippedQuantity_sum
   ,c.inbound_cases_sum != null ? c.inbound_cases_sum : 0 inbound_cases_sum
  FROM c
  JOIN t IN c.WIP_ActivityName
  WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
   AND c.DP_SERVICELINE_KEY IN ('53D60776-D3BD-4E6A-8E37-F591C148294B')
   -- AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
   AND (
    (
     c.DateTimeReceived BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '' = '*' OR '' = '*'
    )
   AND (
    (
     c.ma_ActivityDate_2 BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '*' = '*' OR '' = '*'
    )
   AND (('*'='*') ? c.ma_ActivityDate_1 !=null : ((''='*') ? c.ma_ActivityDate_2 !=null : true) )
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
 
if UPPER(@type)='*'
-- aggregate result form above cases
 
 
-- condition 2:  IF UPPER(@dateType)  = '' or  UPPER(@dateType)  = '*' or UPPER(@dateType)  = null
 
--IF  UPPER(@type)='ASN'
 
 SELECT a.type
 ,a.ActivityName
 ,a.warehouseCode
 ,a.Count = null ? 0 : a.Count Count
 ,a.Date
 ,a.ActivityOrderId
FROM (
 SELECT 'ASN' type
  ,b.ActivityOrderId
  ,b.ActivityName
  ,b.warehouseCode
  ,b.Date
  ,('ASN' = 'ASN' ? Count(1) : ('ASN' = 'LINES' ? sum(b.inbound_line_count) : ('ASN' = 'UNITS' ? sum(b.inbound_ShippedQuantity_sum) : ('ASN' = 'CASES' ? sum(b.inbound_cases_sum) :null)))) Count
 FROM (
  SELECT t.WIPActivityOrderId ActivityOrderId
   ,t.WIP_ActivityName ActivityName
   ,c.warehouseCode
   ,'' = '*' ? d : null Date
   ,c.inbound_line_count != null ? c.inbound_line_count : 0 inbound_line_count
   ,c.inbound_ShippedQuantity_sum != null ? c.inbound_ShippedQuantity_sum : 0 inbound_ShippedQuantity_sum
   ,c.inbound_cases_sum != null ? c.inbound_cases_sum : 0 inbound_cases_sum
  FROM c
  JOIN t IN c.WIP_ActivityName
  JOIN d IN c.ma_ActivityDate_list
  WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
   AND c.DP_SERVICELINE_KEY IN ('53D60776-D3BD-4E6A-8E37-F591C148294B')
   -- AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
   AND (
    (
     c.DateTimeReceived BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '' = '*' OR '' = '*'
    )
   AND (
    (
     d BETWEEN '2022-04-01'
      AND '2022-04-14'
     )
    OR '' = '*' OR '' = '*'
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
 
--IF  UPPER(@type)='LINES'  
 
SELECT a.type
 ,a.ActivityName
 ,a.warehouseCode
 ,a.Count = null ? 0 : a.Count Count
 ,a.Date
 ,a.ActivityOrderId
FROM (
 SELECT 'LINES' type
  ,b.ActivityOrderId
  ,b.ActivityName
  ,b.warehouseCode
  ,b.Date
  ,('LINES' = 'ASN' ? Count(1) : ('LINES' = 'LINES' ? sum(b.inbound_line_count) : ('LINES' = 'UNITS' ? sum(b.inbound_ShippedQuantity_sum) : ('LINES' = 'CASES' ? sum(b.inbound_cases_sum) :null)))) Count
 FROM (
  SELECT t.WIPActivityOrderId ActivityOrderId
   ,t.WIP_ActivityName ActivityName
   ,c.warehouseCode
   ,'' = '*' ? d : null Date
   ,c.inbound_line_count != null ? c.inbound_line_count : 0 inbound_line_count
   ,c.inbound_ShippedQuantity_sum != null ? c.inbound_ShippedQuantity_sum : 0 inbound_ShippedQuantity_sum
   ,c.inbound_cases_sum != null ? c.inbound_cases_sum : 0 inbound_cases_sum
  FROM c
  JOIN t IN c.WIP_ActivityName
  JOIN d IN c.ma_ActivityDate_list
  WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
   AND c.DP_SERVICELINE_KEY IN ('53D60776-D3BD-4E6A-8E37-F591C148294B')
   -- AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
   AND (
    (
     c.DateTimeReceived BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '' = '*' OR '' = '*'
    )
   AND (
    (
     d BETWEEN '2022-04-01'
      AND '2022-04-14'
     )
    OR '' = '*' OR '' = '*'
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
 
 --  IF UPPER(@type)='UNITS'
 
  SELECT a.type
 ,a.ActivityName
 ,a.warehouseCode
 ,a.Count = null ? 0 : a.Count Count
 ,a.Date
 ,a.ActivityOrderId
FROM (
 SELECT 'UNITS' type
  ,b.ActivityOrderId
  ,b.ActivityName
  ,b.warehouseCode
  ,b.Date
  ,('UNITS' = 'ASN' ? Count(1) : ('UNITS' = 'LINES' ? sum(b.inbound_line_count) : ('UNITS' = 'UNITS' ? sum(b.inbound_ShippedQuantity_sum) : ('UNITS' = 'CASES' ? sum(b.inbound_cases_sum) :null)))) Count
 FROM (
  SELECT t.WIPActivityOrderId ActivityOrderId
   ,t.WIP_ActivityName ActivityName
   ,c.warehouseCode
   ,'' = '*' ? d : null Date
   ,c.inbound_line_count != null ? c.inbound_line_count : 0 inbound_line_count
   ,c.inbound_ShippedQuantity_sum != null ? c.inbound_ShippedQuantity_sum : 0 inbound_ShippedQuantity_sum
   ,c.inbound_cases_sum != null ? c.inbound_cases_sum : 0 inbound_cases_sum
  FROM c
  JOIN t IN c.WIP_ActivityName
  JOIN d IN c.ma_ActivityDate_list
  WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
   AND c.DP_SERVICELINE_KEY IN ('53D60776-D3BD-4E6A-8E37-F591C148294B')
   -- AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
   AND (
    (
     c.DateTimeReceived BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '' = '*' OR '' = '*'
    )
   AND (
    (
     d BETWEEN '2022-04-01'
      AND '2022-04-14'
     )
    OR '' = '*' OR '' = '*'
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
 
--   IF UPPER(@type)='CASES'  
 
SELECT a.type
 ,a.ActivityName
 ,a.warehouseCode
 ,a.Count = null ? 0 : a.Count Count
 ,a.Date
 ,a.ActivityOrderId
FROM (
 SELECT 'CASES' type
  ,b.ActivityOrderId
  ,b.ActivityName
  ,b.warehouseCode
  ,b.Date
  ,('CASES' = 'ASN' ? Count(1) : ('CASES' = 'LINES' ? sum(b.inbound_line_count) : ('CASES' = 'UNITS' ? sum(b.inbound_ShippedQuantity_sum) : ('CASES' = 'CASES' ? sum(b.inbound_cases_sum) :null)))) Count
 FROM (
  SELECT t.WIPActivityOrderId ActivityOrderId
   ,t.WIP_ActivityName ActivityName
   ,c.warehouseCode
   ,'' = '*' ? d : null Date
   ,c.inbound_line_count != null ? c.inbound_line_count : 0 inbound_line_count
   ,c.inbound_ShippedQuantity_sum != null ? c.inbound_ShippedQuantity_sum : 0 inbound_ShippedQuantity_sum
   ,c.inbound_cases_sum != null ? c.inbound_cases_sum : 0 inbound_cases_sum
  FROM c
  JOIN t IN c.WIP_ActivityName
  JOIN d IN c.ma_ActivityDate_list
  WHERE c.AccountId IN ('E648FA6F-6253-428E-8AC9-201E3EF83B91')
   AND c.DP_SERVICELINE_KEY IN ('53D60776-D3BD-4E6A-8E37-F591C148294B')
   -- AND c.FacilityId IN ('9CD72E16-9446-46E7-8A25-08D7BEC3176E')
   AND (
    (
     c.DateTimeReceived BETWEEN '2022-04-01 00:00:00.000'
      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-04-14 00:00:00.000'))
     )
    OR '' = '*' OR '' = '*'
    )
   AND (
    (
     d BETWEEN '2022-04-01'
      AND '2022-04-14'
     )
    OR '' = '*' OR '' = '*'
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
 
-- if UPPER(@type)='*'
-- aggregate result form above cases