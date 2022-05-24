-- rpt_Warehouse_Listing

-- Result Set 1a

/*
Target Container - digital_summary_orders
*/

SELECT 
     c.warehouseId
	,c.warehouseCode
	,c.wse_warehouseTimeZone warehouseTimeZone
	,c.wse_addressLine1 addressLine1
	,c.wse_addressLine2 addressLine2
	,c.wse_city city
	,c.wse_stateProvince stateProvince
	,c.wse_postalCode postalCode
	,c.wse_country country
    ,SUM(c.IS_INBOUND = 1 ? 1 : 0) inboundShipmentCount
	,SUM(c.IS_INBOUND = 0 ? 1 : 0) outboundShipmentCount
	
FROM c
WHERE c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
	AND c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
	AND (
		c.DateTimeReceived BETWEEN '2022-01-18 00:00:00.000'
			AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, "2022-02-16 00:00:00.000"))
		)
	AND (is_null(c.OrderCancelledFlag) ? 'N' : c.OrderCancelledFlag) != 'Y'
    and c.IS_INBOUND != null
    	And (c.IS_INBOUND = 1 ?((is_null(c.IS_ASN) ? 0 : c.IS_ASN) = 1): true)
	AND c.FacilityId != null
    AND c.is_deleted = 0
GROUP BY 
    c.warehouseId
	,c.warehouseCode
	,c.wse_warehouseTimeZone
	,c.wse_addressLine1
	,c.wse_addressLine2
	,c.wse_city
	,c.wse_stateProvince
	,c.wse_postalCode
	,c.wse_country


-- Result Set 1b

/*
Target Container - digital_summary_inventory
*/

SELECT 
     c.FacilityId warehouseId
	,c.warehouseCode
	,c.wse_warehouseTimeZone warehouseTimeZone
	,c.wse_addressLine1 addressLine1
	,c.wse_addressLine2 addressLine2
	,c.wse_city city
	,c.wse_stateProvince stateProvince
	,c.wse_postalCode postalCode
	,c.wse_country country 
    ,count(c.LPNNumber) lpnOnHandCount
 FROM c 
where  
c.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529'
and c.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD'
and c.FacilityId != null
and c.is_deleted = 0
group by c.FacilityId 
	,c.warehouseCode
	,c.wse_warehouseTimeZone 
	,c.wse_addressLine1 
	,c.wse_addressLine2 
	,c.wse_city 
	,c.wse_stateProvince 
	,c.wse_postalCode 
	,c.wse_country 

-- Result Set 1c

/*
Target Container - dim_warehouse
*/

SELECT 
    c.warehouseId
	,c.warehouseCode
	,c.warehouseTimeZone
	,c.addressLine1
	,c.addressLine2
	,c.city
	,c.stateProvince
	,c.postalCode
	,c.country
    ,0 inboundShipmentCount
    ,0 outboundShipmentCount
    ,0 lpnOnHandCount    
from c
WHERE c.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
	AND c.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')