-- rpt_Warehouse_Listing

/*
NOTE:
pefrom 
1c left outer
1a left outer
1b
in the backend
*/

-- Result Set 1a

/*
Parameter Requirement info -
-->@DPProductLineKey required 
@DP_SERVICELINE_KEY optional 
@Date.VarStartCreatedDateTime  optional
@Date.VarEndCreatedDateTime  optional

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
WHERE (c.AccountId IN (@AccountKeys.DPProductLineKey) or (c.AccountId = @DPProductLineKey))
	AND (c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey) or (c.DP_SERVICELINE_KEY = @DPServiceLineKey))
	AND (
		c.DateTimeReceived BETWEEN @startDate
			AND DateTimeAdd("ms",-2, DateTimeAdd("dd", 1, @endDate))
		)
	AND (is_null(c.OrderCancelledFlag) ? 'N' : c.OrderCancelledFlag) != 'Y'
    	 and c.IS_INBOUND != null
    	And (c.IS_INBOUND = 1 ?((is_null(c.IS_ASN) ? 0 : c.IS_ASN) = @isASN ): true)
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
Parameter Requirement info -
-->@DPProductLineKey required 
@DP_SERVICELINE_KEY optional 

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
c.AccountId IN (@AccountKeys.DPProductLineKey) or (c.AccountId = @DPProductLineKey))
	AND (c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey) or (c.AccountId = @DPServiceLineKey))
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
Parameter Requirement info -
-->@DPProductLineKey required 
@DP_SERVICELINE_KEY optional 

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
WHERE (c.AccountId IN (@AccountKeys.DPProductLineKey) or (c.AccountId = @DPProductLineKey))
	AND (c.DP_SERVICELINE_KEY IN (@AccountKeys.DPServiceLineKey) or (c.DP_SERVICELINE_KEY = @DPServiceLineKey))