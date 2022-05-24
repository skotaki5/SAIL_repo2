/****** Object:  StoredProcedure [digital].[rpt_Inbound_Activity_Line_Volume_DayLevel_Summary]    Script Date: 1/5/2022 4:29:10 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/****
--AMR
  EXEC [digital].[rpt_Inbound_Activity_Line_Volume_DayLevel_Summary]   @DPProductLineKey = 'E648FA6F-6253-428E-8AC9-201E3EF83B91',@DPServiceLineKey = '53D60776-D3BD-4E6A-8E37-F591C148294B',@DPEntityKey = NULL,@startDate = '2020-11-14',@endDate = '2020-11-20',@dateType='receivedDate',@warehouseId = '*'
--SWR
  EXEC [digital].[rpt_Inbound_Activity_Line_Volume_DayLevel_Summary]   @DPProductLineKey = 'CB692762-2908-4F5F-A7B7-F7E9DBED83F4',@DPServiceLineKey = '*',@DPEntityKey = NULL,@startDate = '2020-11-14',@endDate = '2020-11-20',@dateType='receivedDate',@warehouseId = '*'
--PostSales
  EXEC [digital].[rpt_Inbound_Activity_Line_Volume_DayLevel_Summary]   @DPProductLineKey = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529',@DPServiceLineKey = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD',@DPEntityKey = NULL,@startDate = '2020-11-14',@endDate = '2020-11-20',@dateType='receivedDate',@warehouseId = '*'
****/

CREATE PROCEDURE [digital].[rpt_Inbound_Activity_Line_Volume_DayLevel_Summary] 

@DPProductLineKey varchar(50), @DPServiceLineKey varchar(50), @DPEntityKey varchar(50), 
@startDate date, @endDate date,@dateType varchar(100), @warehouseId varchar(max)

AS

BEGIN

  DECLARE @VarAccountID varchar(50),
          @VarDPServiceLineKey varchar(50),
          @VarDPEntityKey varchar(50),
          @VarStartCreatedDateTime datetime,
          @VarEndCreatedDateTime datetime,
          @NULLCreatedDate varchar(1),
          @VarwarehouseId varchar(max),
		  @VarDateType varchar(100)
		  
  SET @VarAccountID = UPPER(@DPProductLineKey)
  SET @VarDPServiceLineKey = UPPER(@DPServiceLineKey)
  SET @VarDPEntityKey = UPPER(@DPEntityKey)
  SET @VarwarehouseId = UPPER(@warehouseId)
  SET @VarStartCreatedDateTime = @startDate
  SET @VarEndCreatedDateTime = @endDate
  SET @VarEndCreatedDateTime = DATEADD(ms, -2, DATEADD(dd, 1, DATEDIFF(dd, 0, @VarEndCreatedDateTime)))
  SET @VarDateType=UPPER(@dateType)

  IF @VarStartCreatedDateTime IS NULL OR @VarEndCreatedDateTime IS NULL
    SET @NULLCreatedDate = '*'

  IF @DPServiceLineKey IS NULL
    SET @VarDPServiceLineKey = '*'

  IF @DPEntityKey IS NULL
    SET @VarDPEntityKey = '*'


     SELECT SIL.UPSOrderNumber,CONVERT(VARCHAR,SMA.ActivityDate,110) ReceivingDate
	 INTO #temp
	 FROM Summary.DIGITAL_SUMMARY_INBOUND_LINE SIL (NOLOCK)
	 JOIN Summary.DIGITAL_SUMMARY_MILESTONE_ACTIVITY  SMA (NOLOCK) ON SIL.UPSOrderNumber = SMA.UPSOrderNumber
	 															 AND SIL.SourceSystemKey = SMA.SourceSystemKey
	 JOIN master_data.Map_Milestone_Activity MMA (NOLOCK) ON MMA.ActivityName = SMA.ActivityName
														AND MMA.SOURCE_SYSTEM_KEY = SMA.SourceSystemKey
														AND MMA.ActivityCode IN ( 'RECS','REC30','REC90')
     WHERE SIL.AccountId = @VarAccountID
	 AND SMA.ActivityDate between @VarStartCreatedDateTime and @VarEndCreatedDateTime
	 AND (SIL.FacilityId in (select UPPER(TRIM(value)) from string_split(@VarwarehouseId,',')) OR @VarwarehouseId = '*')

     SELECT COUNT(*) AS Total FROM #temp

     SELECT ReceivingDate,COUNT(*) as LINES FROM #temp GROUP BY ReceivingDate ORDER BY 1

END
GO

