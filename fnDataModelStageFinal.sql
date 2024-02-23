-- Drop table DMA.StgReports;

create table DMA.StgReports(
Id int identity(1,1) Primary key,
Title nvarchar(max),
Updates nvarchar(max),
SubmittedTime datetime,
EmployeeEmail nvarchar(max),
ViewEmail nvarchar(max)
);

select * from DMA.StgReports;

-- truncate table DMA.TControlSourceTargetMapping;

insert into DMA.TControlSourceTargetMapping (SrcFileName, SrcSheetName, SrcColumns,DestinationSource,DestinationDataBase,DestinationSchema,DestinationTable,DestinationColumns)
values ('Updates.xlsx', 'Sheet1', 'Update Title', 'Sql Server', 'prospectmanagement', 'DMA', 'StgReports', 'Title'),
('Updates.xlsx', 'Sheet1', 'Update', 'Sql Server', 'prospectmanagement', 'DMA', 'StgReports', 'Updates'),
('Updates.xlsx', 'Sheet1', 'Submitted Time', 'Sql Server', 'prospectmanagement', 'DMA', 'StgReports', 'SubmittedTime'),
('Updates.xlsx', 'Sheet1', 'Submitter''s Email', 'Sql Server', 'prospectmanagement', 'DMA', 'StgReports', 'EmployeeEmail'),
('Updates.xlsx', 'Sheet1', 'Viewers'' Email Item', 'Sql Server', 'prospectmanagement', 'DMA', 'StgReports', 'ViewEmail');

select * from DMA.TControlSourceTargetMapping;

-- truncate table DMA.LogsSourceStage;
select * from DMA.LogsSourceStage;

select * from DMA.StgReports;

-- --    ---------------  Insert data into EmployeesDim from StgReports ------------------------------------------------
-- 220 records at stgReports

select * into #tempDE from DMA.DimEmployees;

-- truncate table #tempDE;

select * from #tempDE;

/*
WITH DataEmployees AS (
    SELECT
        Id,
        EmployeeEmail ,
		ViewEmail,
        ROW_NUMBER() OVER (PARTITION BY  EmployeeEmail,ViewEmail ORDER BY Id) AS rn
    FROM DMA.StgReports
)select count(*) from DataEmployees;


*/


WITH DataEmployees AS (
    SELECT
        Id,
        EmployeeEmail ,
		ViewEmail,
        ROW_NUMBER() OVER (PARTITION BY  EmployeeEmail,ViewEmail ORDER BY Id) AS rn
    FROM DMA.StgReports
)
INSERT INTO #tempDE(EmployeeFirstName, EmployeeLastName, EmployeeEmail)
SELECT Distinct
    PARSENAME(s.EmployeeEmail, 3),
    REPLACE(PARSENAME(REPLACE(s.EmployeeEmail, '@', '.'), 3), '.', ''),
    s.EmployeeEmail
FROM
    DataEmployees s

WHERE
    s.rn = 1 AND NOT EXISTS (
        SELECT 1
        FROM #tempDE t
        WHERE t.EmployeeEmail = s.EmployeeEmail
);


WITH UniqueViewEmails AS (
    SELECT DISTINCT ViewEmail
    FROM DMA.StgReports
)
-- Insert unique ViewEmail values into #tempE
INSERT INTO #tempDE (EmployeeFirstName, EmployeeLastName, EmployeeEmail)
SELECT
    PARSENAME(s.ViewEmail, 3) AS EmployeeFirstName,
    REPLACE(PARSENAME(REPLACE(s.ViewEmail, '@', '.'), 3), '.', '') AS EmployeeLastName,
    s.ViewEmail AS EmployeeEmail
FROM
    UniqueViewEmails s
WHERE
    NOT EXISTS (
        SELECT 1
        FROM #tempDE t
        WHERE t.EmployeeEmail = s.ViewEmail
);
-- ------------------------------Insertion completed for Dim EMployees --------------------------------

-- -------------------------------Insert into DimProject from StageReports ------------------------------
select * into #tempProject from DMA.DimProject;

truncate table #tempProject;

select * from #tempProject;


select distinct Title,SubmittedTime,ViewEmail,EmployeeEmail,Updates into #temp from DMA.StgReports;

-- select  * into #tempRp from DMA.StgReports;

-- select * from #tempRp;

-- Final Best Query Passed all Test Cases ----
Insert into #tempProject (ProjectName)
select Distinct 
Replace(SUBSTRING(Updates, 1, CHARINDEX('Update', Updates) - 1),'Service', '') AS ServiceValue
from #temp
where Updates like 'Service%' and not exists 
(select * from #tempProject where ProjectName = Replace(SUBSTRING(Updates, 1, CHARINDEX('Update', Updates) - 1),'Service', '')
);


update #tempProject
set ProjectName = TRIM(REPLACE(ProjectName, 'Service', ''))


select count(*) from #tempProject;

select * from #tempProject;
-- -------------------------------------------------------------
SELECT DISTINCT
  Trim(Replace(
    CASE WHEN CHARINDEX('Update', Updates) > 0
      THEN SUBSTRING(Updates, 1, CHARINDEX('Update', Updates) - 1)
      ELSE ''
    END,
    'Service', ''
  )) AS ProjectName
FROM DMA.StgReports;


  -- WHERE Updates LIKE 'Service%'
  SELECT DISTINCT
    Replace(SUBSTRING(Updates, 1, CHARINDEX('Update', Updates) - 1),'Service', '') AS ProjectName
  FROM DMA.StgReports
  WHERE Updates LIKE 'Service%'
-- -----------------------Insert and update Final one (not passed all test cases)----------------------------
WITH new_projects AS (
  SELECT DISTINCT
    SUBSTRING(Updates, 1, CHARINDEX('Update', Updates) - 1) AS ProjectName
  FROM DMA.StgReports
  WHERE Updates LIKE 'Service%'
)
MERGE INTO  #tempProject AS target
USING new_projects AS source
ON (target.ProjectName = source.ProjectName)
WHEN Not MATCHED THEN
  INSERT (ProjectName) VALUES (source.ProjectName)
WHEN  MATCHED THEN
    UPDATE SET target.ProjectName = TRIM(REPLACE(source.ProjectName, 'Service', ''));


select * from #tempProject;

/*

select PARSENAME('Service IFRS17 / BR1  ', -1);

select replace(PARSENAME( REPLACE('Service IFRS17 / BR1', ' / ', '.'), 2),'Service', ''); --  IFRS17

select len('Service IFRS17 / BR1');

select replace(PARSENAME( REPLACE( ProjectName, ' / ', '.'), 2),'Service', '') from #tempProject;

select TRIM(REPLACE(ProjectName, 'Service', '')) from #tempProject; -- FDM/SMall Change (Service removed)

select Trim(replace(ProjectName, 'Service', '')) from #tempProject

select SUBSTRING(ProjectName,  1,CHARINDEX('', ProjectName))as res from #tempProject;

*/

/*
Rough 
select  * into #tempR from DMA.StgReports;

ALTER TABLE #temPR ADD NewColumn nvarchar(max);


select * from #tempR;


select EmployeeEmail, Updates from #tempR where Updates not like 'Team%';

select EmployeeEmail , Updates from #tempR where Updates like '[Service]% [Update]%' ;

-- Result

select EmployeeEmail, 
SUBSTRING(Updates, 1, CHARINDEX('Update', Updates) - 1) AS ServiceValue
from #tempR
where Updates like 'Service%';

select CHARINDEX('Service Health RAG', Updates) as res from #tempR;

select SUBSTRING(Updates, 1, CHARINDEX('Service Health RAG', Updates) - 1)as res from #tempR where Updates like 'Service%';

-- SUBSTRING(Updates, 1, CHARINDEX('/', Updates) - 1)


*/

-- ---------------------------Insert into DimService ----------------------------------------
select * into #tempService from DMA.DimService;

truncate table #tempService;

select count(*) from #tempService where RagName = '';

select * from #tempService;

-- -------------------------------------Final Insert Service passed all Test Cases --------
INSERT INTO #tempService (RagName)
SELECT DISTINCT
  CASE 
    WHEN SUBSTRING(Updates, CHARINDEX('Service Health RAG', Updates) + LEN('Service Health RAG') + 1, LEN(Updates)) IS NULL OR
         SUBSTRING(Updates, CHARINDEX('Service Health RAG', Updates) + LEN('Service Health RAG') + 1, LEN(Updates)) = ''
    THEN 'unknown'
    ELSE SUBSTRING(Updates, CHARINDEX('Service Health RAG', Updates) + LEN('Service Health RAG') + 1, LEN(Updates))
  END AS RagName
FROM #tempRp AS tr
WHERE Updates LIKE 'Service%' 
  AND CHARINDEX('Service Health RAG', Updates) > 0 
  AND NOT EXISTS (
    SELECT 1 
    FROM #tempService AS ts
    WHERE 
      (CASE 
        WHEN SUBSTRING(tr.Updates, CHARINDEX('Service Health RAG', tr.Updates) + LEN('Service Health RAG') + 1, LEN(tr.Updates)) IS NULL OR
             SUBSTRING(tr.Updates, CHARINDEX('Service Health RAG', tr.Updates) + LEN('Service Health RAG') + 1, LEN(tr.Updates)) = ''
        THEN 'unknown'
        ELSE SUBSTRING(tr.Updates, CHARINDEX('Service Health RAG', tr.Updates) + LEN('Service Health RAG') + 1, LEN(tr.Updates))
      END) = ts.RagName
  )
  AND (CASE 
        WHEN SUBSTRING(tr.Updates, CHARINDEX('Service Health RAG', tr.Updates) + LEN('Service Health RAG') + 1, LEN(tr.Updates)) IS NULL OR
             SUBSTRING(tr.Updates, CHARINDEX('Service Health RAG', tr.Updates) + LEN('Service Health RAG') + 1, LEN(tr.Updates)) = ''
        THEN 'unknown'
        ELSE SUBSTRING(tr.Updates, CHARINDEX('Service Health RAG', tr.Updates) + LEN('Service Health RAG') + 1, LEN(tr.Updates))
      END) IS NOT NULL;  





-- -------------------------------------

Insert into #tempService(RagName)
select Distinct
SUBSTRING(Updates, CHARINDEX('Service Health RAG', Updates) + LEN('Service Health RAG') + 1, LEN(Updates)) AS ServiceValue
from #tempRp
where Updates like 'Service%'And CHARINDEX('Service Health RAG', Updates) > 0 AND NOT EXISTS (
  SELECT * FROM #tempService
  WHERE RagName = SUBSTRING(Updates, CHARINDEX('Service Health RAG', Updates) + LEN('Service Health RAG') + 1, LEN(Updates))
)

-- -------------------------------------
INSERT INTO #tempService (RagName)
SELECT DISTINCT ServiceValue
FROM (
    SELECT 
        SUBSTRING(Updates, CHARINDEX('Service Health RAG', Updates) + LEN('Service Health RAG') + 1, LEN(Updates)) AS ServiceValue
    FROM #tempRp
    WHERE Updates LIKE 'Service%' AND CHARINDEX('Service Health RAG', Updates) > 0
) AS Subquery
WHERE NOT EXISTS (
    SELECT 1
    FROM #tempService
    WHERE RagName = Subquery.ServiceValue
) AND Subquery.ServiceValue IS NOT NULL;


select * from #tempService;

UPDATE #tempService
SET RagName = CASE
  WHEN RagName IS NULL OR RagName = '' THEN 'None'
  ELSE RagName
END;
-- ---------------- Insert DimService --------------------
WITH new_ragnames AS (
  SELECT DISTINCT
    SUBSTRING(Updates, CHARINDEX('Service Health RAG', Updates) + LEN('Service Health RAG') + 1, LEN(Updates)) AS RagName
  FROM #tempRp
  WHERE Updates LIKE 'Service%'
)
MERGE INTO #tempService AS target
USING new_ragnames AS source
ON (target.RagName = source.RagName)
WHEN MATCHED THEN
  UPDATE SET target.RagName = CASE
    WHEN TRIM(target.RagName) = '' THEN 'None' 
    ELSE target.RagName 
  END
WHEN NOT MATCHED THEN
  INSERT (RagName) VALUES (source.RagName);



/*
where not exists (select 1 from olap.coupon_d  d where d.coupon_id = s.cupon_id);
*/


select 
SUBSTRING(Updates, CHARINDEX('Update', Updates) + LEN('Update') + 1, len(Updates) -  LEN('Update') - LEN(' Service Health RAG')) AS ServiceValue
from #tempRp
where Updates like 'Service%';


-- 
SELECT 
  RTRIM(SUBSTRING(Updates, CHARINDEX('Update', Updates) + LEN('Update') + 1, LEN(Updates) - LEN('Update'))) AS ServiceValue
FROM #tempRp
WHERE Updates LIKE 'Service%';

-- 
SELECT 
  SUBSTRING(Updates,  1, CHARINDEX('Service Health RAG', Updates) - 1) AS ServiceValue
FROM #tempRp
WHERE Updates LIKE 'Service%';

-- 
SELECT 
  SUBSTRING(Updates, CHARINDEX('Update', Updates) + LEN('Update') + 1, LEN(Updates)) AS ServiceValue
FROM #tempRp
WHERE Updates LIKE 'Service%';






select Updates from #tempRp where Updates like 'Service%';
-- -----------------------------------------Insert into FactReports--------------------------------------------------
select * into #tempF from DMA.FactReports;

select distinct Title,SubmittedTime,ViewEmail,EmployeeEmail,Updates into #temp from DMA.StgReports;

truncate table #tempF;

select * from #tempF;

select * from #tempRp;

select *
from	#temp	
-- Final one
SELECT
  SUBSTRING(Updates,
    CHARINDEX('Update', Updates) + LEN('Update') + 1,
    CHARINDEX('Service Health RAG', Updates)  - CHARINDEX('Update', Updates) - LEN('Update') -1
  ) AS ServiceValue
FROM #tempRp
WHERE Updates LIKE 'Service%';
-- --

insert into #tempF(Updates , SubmittedTime, DDateKey)
select SUBSTRING(stg.Updates, CHARINDEX('Update', stg.Updates) + LEN('Update') + 1, LEN(stg.Updates)) AS ServiceValue,
stg.SubmittedTime,
dd.DateKey
from #tempRp stg
join DMA.DimDate dd on convert(Date, stg.SubmittedTime) = dd.Date

-- test for serviceName insert 
insert into #tempF(Updates,SubmittedTime ,DDateKey,DProjectId,DEmployeesId,ViewersID,DServiceKey)
select  Distinct REPLACE(REPLACE(REPLACE(REPLACE(
    SUBSTRING(Updates,
        CHARINDEX('Update', Updates) + LEN('Update') + 1,
        CHARINDEX('Service Health RAG', Updates) - CHARINDEX('Update', Updates) - LEN('Update') - 1
    ), '<div>', ''), '</div>', ''), '<br>', ''), '&nbsp;', '') AS ServiceValue,
stg.SubmittedTime
--dd.DateKey,
--dp.Id,
--de.EmployeeId,
--dv.EmployeeId,
--ds.ServiceId
from #temp stg
join DMA.DimDate dd on convert(Date, stg.SubmittedTime) = dd.Date
join DMA.DimProject dp on Replace(SUBSTRING(stg.Updates, 1, CHARINDEX('Update', stg.Updates) - 1),'Service', '') = dp.ProjectName
join DMA.DimEmployees de on stg.EmployeeEmail = de.EmployeeEmail 
join DMA.DimEmployees dv on stg.ViewEmail = dv.EmployeeEmail 
join DMA.DimService ds on SUBSTRING(Updates, CHARINDEX('Service Health RAG', Updates) + LEN('Service Health RAG') + 1, LEN(Updates)) = ds.RagName 
WHERE stg.Updates LIKE 'Service%' and NOT EXISTS (select 1 from #tempF tf where tf.Updates = REPLACE(REPLACE(REPLACE(REPLACE(
        SUBSTRING(stg.Updates,
            CHARINDEX('Update', stg.Updates) + LEN('Update') + 1,
            CHARINDEX('Service Health RAG', stg.Updates) - CHARINDEX('Update', stg.Updates) - LEN('Update') - 1
        ), '<div>', ''), '</div>', ''), '<br>', ''), '&nbsp;', '')
    AND tf.SubmittedTime = stg.SubmittedTime) ;


select count(*) from #tempF where DDateKey = '20231211' -- 6

-- -----------------------------------------------
update #tempF
set Updates = replace(replace(replace(replace(Updates, '<div>', ''), '</div>', ''), '<br>', ''), '&nbsp', '');

select * from #tempF;
-- ----------------------------------------------
select Substring('name', 1, len('name')-1); -- nam
-- ---------------------------------------------------------------------------------------------
-- stages check
select Updates from DMA.StgReports;

select SUBSTRING(Updates, CHARINDEX('Update', Updates)+ LEN('Update'), len(Updates)) from DMA.StgReports;



update DMA.StgReports
set Updates = replace(replace(replace(replace(Updates, '<div>', ''), '</div>', ''), '<br>', ''), '&nbsp', '');



-- Dimesnions Check

select * from DMA.DimEmployees;


