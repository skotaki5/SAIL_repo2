/****** Object:  StoredProcedure [digital].[rpt_Inbound_Shipment_Detail_Milestone]    Script Date: 2/17/2022 5:11:26 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/****
--AMR
EXEC [digital].[rpt_Inbound_Shipment_Detail_Milestone]    @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@UPSShipmentNumber = '45297'
--SWR
EXEC [digital].[rpt_Inbound_Shipment_Detail_Milestone]    @DPProductLineKey = '870561E1-A974-483B-AA0D-A724C5D402C9',@DPServiceLineKey = '150E5128-B3E3-44A1-BF6C-CD2E4D9856DA',@DPEntityKey = NULL,@UPSShipmentNumber = '6391'
--Cambium
EXEC [digital].[rpt_Inbound_Shipment_Detail_Milestone]    @DPProductLineKey = 'A10F512B-C7F4-42A0-909C-21DD95A7D921',@DPServiceLineKey = '53DDDBC2-FB5A-4E70-A92E-8312DC9FDD05',@DPEntityKey = NULL,@UPSShipmentNumber = '6391'
****/

/*****
Harsha                  02/13/2022                      Adding table alias wherver required
*********/


CREATE PROCEDURE [digital].[rpt_Inbound_Shipment_Detail_Milestone] @DPProductLineKey varchar(50), @DPServiceLineKey varchar(50) = NULL, @DPEntityKey varchar(50) = NULL, @UPSShipmentNumber varchar(50)
AS

BEGIN

  DECLARE  @VarAccountID varchar(50)
          ,@VarDPServiceLineKey varchar(50)
          ,@VarDPEntityKey varchar(50)
		  ,@SS_MG int

  SET @VarAccountID         = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey  = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey       = UPPER(@DPEntityKey)
  SET @SS_MG                = 1011

  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*'

  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'

	  SELECT     TBL.ShipmentMileStones
			,MAX(TBL.MilesStoneEstimatedDateTime)  AS MilesStoneEstimatedDateTime
			,MAX(TBL.MilesStoneCompletionDateTime) AS MilesStoneCompletionDateTime
			,MAX(TBL.activityCount)                AS activityCount
			,    TBL.templateType
	FROM
	(
	   SELECT
		 M.MilestoneName                                  AS ShipmentMileStones
		,M.MilestoneOrder			                      
		,MA.PlannedMilestoneDate                          AS MilesStoneEstimatedDateTime
		,CASE WHEN MA.MilestoneCompletionFlag = 'Y' 
		      THEN COALESCE(MA.MilestoneDate, MA.ActivityDate) 
	     END                                              AS MilesStoneCompletionDateTime
		,COALESCE(MA1.activityCount, 0)                   AS activityCount
		,MTM.TransactionTypeName                          AS templateType 
	  FROM [Summary].[DIGITAL_SUMMARY_MILESTONE]          (NOLOCK) M
 LEFT JOIN [Summary].[DIGITAL_SUMMARY_MILESTONE_ACTIVITY] (NOLOCK) MA 
	                                                            ON M.UPSOrderNumber = MA.UPSOrderNumber	
															   AND M.MilestoneOrder = MA.MilestoneOrder			
				                                               AND MA.SourceSystemKey = CASE WHEN M.SourceSystemKey = @SS_MG THEN MA.SourceSystemKey ELSE M.SourceSystemKey END	
 LEFT JOIN (SELECT COUNT(1) AS activityCount,MA.UPSOrderNumber,MA.SourceSystemKey,MA.MilestoneName
						FROM Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA
						WHERE MA.UPSOrderNumber = @UPSShipmentNumber
						GROUP BY MA.UPSOrderNumber,MA.SourceSystemKey,MA.MilestoneName
				 ) MA1 ON M.UPSOrderNumber = MA1.UPSOrderNumber
					   AND MA.SourceSystemKey = CASE WHEN M.SourceSystemKey = @SS_MG THEN MA.SourceSystemKey ELSE M.SourceSystemKey END
					   AND M.MilestoneName = MA1.MilestoneName
 LEFT JOIN master_data.Map_TransactionType_Milestone (NOLOCK) MTM ON MA.TransactionTypeId=MTM.TransactionTypeId
	  WHERE M.AccountId = @VarAccountID
	  AND M.UPSOrderNumber = @UPSShipmentNumber
	  AND M.MilestoneName <> 'ALERT'
	  AND (M.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
	  --AND (ISNULL(M.DP_ORGENTITY_KEY,'') = @VarDPEntityKey        OR @VarDPEntityKey = '*')	  
  
  ) TBL
  GROUP BY TBL.ShipmentMileStones,
           TBL.MilestoneOrder,
		   TBL.templateType
		   
  ORDER BY TBL.MilestoneOrder

  SELECT
    MA.ActivityName AS AlertCode,
    COALESCE(MA.MilestoneDate, MA.ActivityDate) AS AlertDateTime,
    '' AS AlertMessageLatest,
    MA.MilestoneName AS Milestone
  FROM [Summary].[DIGITAL_SUMMARY_ORDERS]                (NOLOCK) O
  LEFT JOIN [Summary].DIGITAL_SUMMARY_MILESTONE_ACTIVITY (NOLOCK) MA ON O.UPSOrderNumber = MA.UPSOrderNumber
																	AND O.SourceSystemKey = MA.SourceSystemKey

  WHERE O.AccountId = @VarAccountID
  AND (O.DP_SERVICELINE_KEY = @VarDPServiceLineKey OR @VarDPServiceLineKey = '*')
  --AND (ISNULL(O.DP_ORGENTITY_KEY,'') = @VarDPEntityKey OR @VarDPEntityKey = '*')
  AND (O.UPSOrderNumber = @UPSShipmentNumber)
  AND MA.MilestoneName = 'ALERT'
  

END
GO

