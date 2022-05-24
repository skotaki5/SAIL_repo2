/****** Object:  StoredProcedure [digital].[rpt_Exception_History_Shipments]    Script Date: 12/8/2021 1:37:39 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE  PROCEDURE [digital].[rpt_Exception_History_Shipments] 

@DPProductLineKey varchar(50),
@DPServiceLineKey varchar(50),
@DPEntityKey varchar(50),
@UPSShipmentNumber varchar(50),
@shipmentType varchar(10)
        
AS

BEGIN
DECLARE  @VarAccountID VARCHAR(50)
		,@VarDPServiceLineKey VARCHAR(50)
		,@VarDPEntityKey VARCHAR(50)
		,@VarShipmentType varchar(50)
		,@NULLShipmentType varchar(1),
		 @isInbound           INT
		
SET @VarAccountID = UPPER(@DPProductLineKey)
SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
SET @VarDPEntityKey = UPPER(@DPEntityKey)
SET @VarShipmentType=UPPER(@shipmentType)

IF @DPServiceLineKey IS NULL
	SET @VarDPServiceLineKey = '*'

IF @DPEntityKey IS NULL
	SET @VarDPEntityKey = '*'

IF @VarShipmentType='' OR @VarShipmentType IS NULL
     SET @NULLShipmentType = '*'
  ELSE
   SET @isInbound = CASE WHEN @VarShipmentType='INBOUND' THEN 1
                         WHEN @VarShipmentType='OUTBOUND' THEN 0 
						 WHEN @VarShipmentType='MOVEMENT' THEN 2
                END

IF(@VarShipmentType='INBOUND' OR @VarShipmentType='MOVEMENT')
BEGIN
SELECT 
M.ActivityName AS ExceptionType,
OTZ_ExceptionCreatedDate AS creationDateTime,
ExceptionReason AS exceptionReason,
ExceptionPrimaryIndicator AS PrimaryIndicator
FROM [Summary].DIGITAL_SUMMARY_EXCEPTIONS (NOLOCK) EX 
LEFT JOIN [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O
         ON O.UPSOrderNumber = EX.UPSOrderNumber
		 --AND O.SourceSystemKey = EX.SourceSystemKey
LEFT JOIN master_data.Map_Milestone_Activity (NOLOCK)M ON M.ActivityCode = EX.ExceptionEvent
                                              AND M.SOURCE_SYSTEM_KEY = EX.SourceSystemKey
WHERE EX.[UPSOrderNumber] = @UPSShipmentNumber 
        --AND O.AccountId = @VarAccountID 
		--AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*') 	

		
		ORDER BY UTC_ExceptionCreatedDate DESC

END
IF(@VarShipmentType='OUTBOUND')
BEGIN
SELECT 
M.ActivityName AS ExceptionType,
OTZ_ExceptionCreatedDate AS creationDateTime,
ExceptionReason AS exceptionReason,
ExceptionPrimaryIndicator AS PrimaryIndicator
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O
INNER JOIN [Summary].DIGITAL_SUMMARY_EXCEPTIONS (NOLOCK) EX 
        ON O.UPSTransportShipmentNumber = EX.UPSOrderNumber
LEFT JOIN master_data.Map_Milestone_Activity (NOLOCK)M ON M.ActivityCode = EX.ExceptionEvent
                                              AND M.SOURCE_SYSTEM_KEY = EX.SourceSystemKey

		 --AND O.SourceSystemKey = EX.SourceSystemKey
WHERE O.AccountId = @VarAccountID 
		AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*') 	
		AND  O.[UPSOrderNumber] = @UPSShipmentNumber 
  UNION
		
SELECT   
M.ActivityName AS ExceptionType,  
UTC_ExceptionCreatedDate AS creationDateTime,  
ExceptionReason AS exceptionReason,  
ExceptionPrimaryIndicator AS PrimaryIndicator  
FROM [Summary].[DIGITAL_SUMMARY_ORDERS] (NOLOCK) O  
INNER JOIN [Summary].DIGITAL_SUMMARY_EXCEPTIONS (NOLOCK) EX   
        ON O.UPSOrderNumber = EX.UPSOrderNumber  
LEFT JOIN master_data.Map_Milestone_Activity (NOLOCK) M ON M.ActivityCode = EX.ExceptionEvent  
                                              AND M.SOURCE_SYSTEM_KEY = EX.SourceSystemKey  
  
   --AND O.SourceSystemKey = EX.SourceSystemKey  
WHERE EX.SourceSystemKey=1019
  AND O.AccountId = @VarAccountID   
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')    
  AND  O.[UPSOrderNumber] = @UPSShipmentNumber

ORDER BY creationDateTime DESC

END
END

GO

