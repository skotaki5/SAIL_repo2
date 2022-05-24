/****** Object:  StoredProcedure [digital].[rpt_Symphony_Track_Shipment_Summary]    Script Date: 3/16/2022 5:13:32 PM ******/
SET ANSI_NULLS ON
GO
 
SET QUOTED_IDENTIFIER ON
GO
 
 
 
/****
 
CHANGE LOG
----------
CHANGED BY              DATE                SPRINT                  CHANGES
SAGAR               12-31-2021               46th                   Unauthenticated user 12583 UI to enter UPS Shipment# for Tracking
Shalaka             01-17-2022              UPSGLD 13166            Replace @SerachBy by @varSearchBy in Case statement
Piyali              01-19-2022              UPSGLD 13166            Produce the Result Sets for @cnt_record = 0 along with @cnt_record > 1
Sheetal             02/16/2022              UPSGLD-14069            Altered the logic to get result with any customerreferencenumber values
 
EXEC [digital].[rpt_symphony_Track_Shipment_Summary]
@SearchBy = 'UPSSHIPMENTNUMBER',
@SearchValue = '11564106';
 
EXEC [digital].[rpt_symphony_Track_Shipment_Summary]
@SearchBy = 'UPSSHIPMENTNUMBER',
@SearchValue = '86509065';
 
EXEC [digital].[rpt_symphony_Track_Shipment_Summary]
@SearchBy = 'CUSTOMERREFERENCENUMBER',
@SearchValue = '0080018421'
 
EXEC [digital].[rpt_symphony_Track_Shipment_Summary]
@SearchBy = 'UPSSHIPMENTNUMBER',
@SearchValue = '0836937074'
 
EXEC [digital].[rpt_symphony_Track_Shipment_Summary]
@SearchBy = 'CustomerReferenceNumber',
@SearchValue = '0080016934';
 
****/
 
CREATE PROC [digital].[rpt_Symphony_Track_Shipment_Summary]
@SearchBy varchar(max),
@SearchValue varchar(max),
@accountNumber varchar(50) = NULL,
@shipmentnumber varchar(50)  = NULL
 
 
AS
 
BEGIN
 
DECLARE
        @varSearchBy varchar(max),
        @varSearchValue varchar(max),
        --@varJsonSearchValue varchar(max) ,-->MultiSearch
        @cnt_record int,
        @cnt_search int, -->MultiSearch
        @VarAccountID VARCHAR(50),
        @VarOrderNumber VARCHAR(50),
        @VaraccountNumber varchar(50) = NULL,
        @Varshipmentnumber varchar(50)  = NULL
 
    SET @varSearchBy = UPPER(@SearchBy);
    SET @varSearchValue = '';
    --SET @varJsonSearchValue = @SearchValue;-->MultiSearch
    SET @cnt_record  = 0
    SET @cnt_search  = 0   -->MultiSearch
    SET @VarAccountID = UPPER(@accountNumber);
    SET @VarOrderNumber = UPPER(@shipmentnumber);
 
    --Store Search Values in Temp Table (MultiSearch)
    --SELECT TRIM(SearchValue) AS SearchValue
    --  INTO #TMPSearch
    --FROM OPENJSON(@varJsonSearchValue)
    --WITH (
    --SearchValue NVARCHAR(100) 'strict $.searchValue'
    --   )
 
 
    --For single search values, use @varSearchValue for all the Summary validation query -->MultiSearch
    --SET @varSearchValue = (SELECT UPPER(SearchValue) FROM #TMPSearch)
    SET @varSearchValue = UPPER(@SearchValue)
 
--UPSGLD-14069
 
CREATE TABLE #Temp
(
ShipmentNumber                              Varchar(150)
,AccountID                                  Varchar(255)
,accountNumber                              nvarchar(100)
,carrierService                             Varchar(255)
,shipmentDestination_addressLine1           nVarchar(1024)
,shipmentDestination_addressLine2           nVarchar(1024)
,shipmentDestination_city                   nVarchar(510)
,shipmentDestination_stateProvince          nVarchar(510)
,shipmentDestination_postalCode             nVarchar(510)
,shipmentDestination_country                nVarchar(510)
,CreatedOn                                  datetime
)
 
--UPSGLD-14069
 
 
IF @varSearchBy = 'UPSSHIPMENTNUMBER'
BEGIN
 
INSERT INTO #Temp   
SELECT DISTINCT
            UPSOrderNumber AS ShipmentNumber,
            AccountId AS AccountID,
            Account_number AS accountNumber,
            Carrier AS carrierService,
            DestinationAddress1 AS shipmentDestination_addressLine1,
            DestinationAddress2 AS shipmentDestination_addressLine2,
            DestinationCity AS shipmentDestination_city,
            DestinationProvince AS shipmentDestination_stateProvince,
            DestinationPostalcode AS shipmentDestination_postalCode,
            DestinationCountry AS shipmentDestination_country,
            ScheduleShipmentDate AS CreatedOn
FROM Summary.DIGITAL_SUMMARY_ORDERS (NOLOCK)
WHERE UPSOrderNumber COLLATE SQL_Latin1_General_CP1_CI_AS  = @varSearchValue COLLATE SQL_Latin1_General_CP1_CI_AS      --UPSGLD-13166
 
END
 
--UPSGLD-14069
 
IF @varSearchBy = 'CUSTOMERREFERENCENUMBER'
BEGIN
 
INSERT INTO #Temp
SELECT DISTINCT
            UPSOrderNumber AS ShipmentNumber,
            AccountId AS AccountID,
            Account_number AS accountNumber,
            Carrier AS carrierService,
            DestinationAddress1 AS shipmentDestination_addressLine1,
            DestinationAddress2 AS shipmentDestination_addressLine2,
            DestinationCity AS shipmentDestination_city,
            DestinationProvince AS shipmentDestination_stateProvince,
            DestinationPostalcode AS shipmentDestination_postalCode,
            DestinationCountry AS shipmentDestination_country,
            ScheduleShipmentDate AS CreatedOn
FROM Summary.DIGITAL_SUMMARY_ORDERS (NOLOCK) FTO
WHERE ( FTO.ORDER_REF_1_VALUE = @varSearchValue  COLLATE SQL_Latin1_General_CP1_CI_AS
                OR FTO.ORDER_REF_2_VALUE = @varSearchValue  COLLATE SQL_Latin1_General_CP1_CI_AS
                OR FTO.ORDER_REF_3_VALUE = @varSearchValue  COLLATE SQL_Latin1_General_CP1_CI_AS
                OR FTO.ORDER_REF_4_VALUE = @varSearchValue  COLLATE SQL_Latin1_General_CP1_CI_AS
                OR FTO.ORDER_REF_5_VALUE = @varSearchValue  COLLATE SQL_Latin1_General_CP1_CI_AS
        )
 
 
END
 
--UPSGLD-14069
 
 
--Result Set1
 
SELECT count(*) totalCount FROM #TEMP
 
--Result Set2
 
SELECT * FROM #TEMP
 
 
END
GO
 
   