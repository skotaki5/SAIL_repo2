/****** Object:  StoredProcedure [dbo].[RPT_MG_LOAD_DOCUMENTS]    Script Date: 1/10/2022 10:45:47 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




/****
	EXEC [dbo].[RPT_MG_LOAD_DOCUMENTS] LD3803353
****/

CREATE PROCEDURE [dbo].[RPT_MG_LOAD_DOCUMENTS] @Load_ID varchar(50)
AS
BEGIN
 DECLARE @VarLoadID varchar(50),
	@VarShipmentID varchar(50),
	@VarMGDocID bigint;

 SET @VarLoadID = @Load_ID;
 SET @VarShipmentID = @Shipment_ID;
 SET @VarMGDocID = @MGDOCID;
 
 IF @Load_ID IS NULL
    SET @VarLoadID = '*';

IF @Shipment_ID IS NULL
	SET @VarShipmentID = '*';
	
WITH MGDOCID_CTE (MGDOCID)
AS	(select MAX(MGDOC_ID) as MGDOC_ID
	from [MERCURYGATE].[MG_TMS_LOAD_DOCUMENTS]
	where  (Shipment_ID = @VarShipmentID OR @VarShipmentID = '*') AND (Load_ID = @VarLoadID OR @VarLoadID = '*') AND (MGDOC_ID = @VarMGDocID OR @VarMGDocID = 0) 
	AND DOC_TYPE not in ('Tender','Historical Tender') AND CreatedBy <> 'UPSGlobalHubLoadID'
	group by Shipment_ID,Doc_TYPE,[FileName])


select 
MGDOC_ID,
Load_ID,
Shipment_ID,
DOC_TYPE
,[FileName]
,DisplayType
,UniqueFilename 
,Comments
,CreationDate
from [MERCURYGATE].[MG_TMS_LOAD_DOCUMENTS]
where MGDOC_ID in (
	select MGDOCID from MGDOCID_CTE
)

END
GO

