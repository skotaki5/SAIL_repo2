-- RPT_MG_LOAD_DOCUMENTS

-- Result Set 1
/*
Parameter Requirement info -
->@Load_ID required
Target Container - 
*/

select 
c.MGDOC_ID,
c.Load_ID,
c.Shipment_ID,
c.DOC_TYPE,
c.[FileName],
c.DisplayType,
c.UniqueFilename ,
c.Comments,
c.CreationDate,
from c
where MGDOC_ID in Array(
	select MAX(c.MGDOC_ID) as MGDOC_ID
	from c
	where  c.(Load_ID = @Load_ID )  
	AND c.DOC_TYPE not in ('Tender','Historical Tender') AND CreatedBy <> 'UPSGlobalHubLoadID'
	group by Shipment_ID,Doc_TYPE,[FileName]
)