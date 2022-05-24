--rpt_Symphony_Track_Shipment_Summary

--Result Set 1

/*
Target Container - digital_summary_orders
*/

-- IF @SearchBy = 'UPSSHIPMENTNUMBER'

SELECT count(1) totalResults FROM c
WHERE c.UPSOrderNumber ='86509065'

-- IF @SearchBy= 'CUSTOMERREFERENCENUMBER'

SELECT count(1)  totalResults FROM c
WHERE (c.referenceNumber1 ='3267736' OR c.referenceNumber2 ='' OR c.referenceNumber3 ='' OR c.referenceNumber4 ='' OR c.referenceNumber5 ='')

--Result Set 2

/*
Target Container - digital_summary_orders
*/

-- IF @SearchBy = 'UPSSHIPMENTNUMBER'

SELECT DISTINCT
            c.UPSOrderNumber AS ShipmentNumber,
            c.AccountId AS AccountID,
            c.Accountnumber AS accountNumber,
            c.Carrier AS carrierService,
            c.shipmentDestination_addressLine1,
            c.shipmentDestination_addressLine2,
            c.shipmentDestination_city,
            c.shipmentDestination_stateProvince,
            c.shipmentDestination_postalCode,
            c.shipmentDestination_country,
            c.shipmentCreateDateTime AS CreatedOn
            FROM c
WHERE c.UPSOrderNumber ='86509065'

-- IF @SearchBy= 'CUSTOMERREFERENCENUMBER' 

SELECT DISTINCT
            c.UPSOrderNumber AS ShipmentNumber,
            c.AccountId AS AccountID,
            c.Accountnumber AS accountNumber,
            c.Carrier AS carrierService,
            c.shipmentDestination_addressLine1,
            c.shipmentDestination_addressLine2,
            c.shipmentDestination_city,
            c.shipmentDestination_stateProvince,
            c.shipmentDestination_postalCode,
            c.shipmentDestination_country,
            c.shipmentCreateDateTime AS CreatedOn
            FROM c
WHERE (c.referenceNumber1 ='@SearchValue' OR c.referenceNumber2 ='@SearchValue' OR c.referenceNumber3 ='@SearchValue' OR c.referenceNumber4 ='@SearchValue' OR c.referenceNumber5 ='@SearchValue')
