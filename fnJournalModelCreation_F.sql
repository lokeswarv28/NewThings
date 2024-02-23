--select * from dma.stgreports order by SubmittedTime desc;

--select * from dma.TControlSourceTargetMapping;

--select * from DMA.DimDate;

--select * from DMA.DimEmployee; 

---- truncate table DMA.DimProject;

--select * from DMA.DimProject; -- 10

--select * from DMA.DimRag; -- 4

--drop table DMA.FactReports

--DROP TABLE DMA.DimProject;
--drop table DMA.DimClient

--DROP TABLE DMA.DimEmployee 
--DROP TABLE DMA.DimRag;

create table DMA.DimEmployee(
Pk_EmployeeId int identity(1,1) Primary key,
EmployeeFirstName nvarchar(max),
EmployeeLastName nvarchar(max),
EmployeeEmail nvarchar(max),
CreatedDate datetime DEFAULT getDate()
);

create table DMA.DimClient(
Pk_ClientId int identity(1,1) primary key,
ClientName nvarchar(max),
CreatedDate datetime DEFAULT getDate()

);

create table DMA.DimProject(
Pk_ProjectId int identity(1,1) Primary key,
ProjectName nvarchar(max),
Fk_ClientId int,
CreatedDate datetime DEFAULT getDate(),
FOREIGN KEY (Fk_ClientId) REFERENCES DMA.DimClient(Pk_ClientId),
);

create table DMA.DimRag(
Pk_ServiceId int identity(1,1) primary key ,
RagName nvarchar(max),
Description nvarchar(max),
CreatedDate datetime DEFAULT getDate()
);

-- Drop table DMA.FactReports;

create table DMA.FactReports(
Pk_Id int identity(1,1) Primary key,
Updates nvarchar(max),
SubmittedTime datetime,
UpdatesRichText nvarchar(max),
Fk_ProjectId int,
Fk_EmployeesId int,
Fk_ViewersID int,
Fk_DateKey int,
Fk_ServiceKey int,
CreatedDate datetime DEFAULT getDate(),
FOREIGN KEY (Fk_EmployeesId) REFERENCES DMA.DimEmployee(Pk_EmployeeId),
FOREIGN KEY (Fk_ViewersID) REFERENCES DMA.DimEmployee(Pk_EmployeeId),
FOREIGN KEY (Fk_DateKey) REFERENCES DMA.DimDate(Pk_DateKey),
FOREIGN KEY (Fk_ProjectId) REFERENCES DMA.DimProject(Pk_ProjectId),
FOREIGN KEY (Fk_ServiceKey) REFERENCES DMA.DimRag(Pk_ServiceId)
);


-- --------------------------------------------------------------------------
--select * from DMA.DimDate;

select * from DMA.DimEmployee; 

select * from DMA.DimProject; 



select * from DMA.DimRag; 


select * from DMA.FactReports order by SubmittedTime desc ; 

truncate table DMA.FactReports;

--update DMA.FactReports
--set Updates = ltrim(Updates)

select * from DMA.StgReports;

truncate table DMA.StgReports;


---- replace(replace(replace(ProjectName,Char(13),''),Char(10),''),Char(9),'')

INSERT INTO dma.FactReports (Updates, SubmittedTime,UpdatesRichText, Fk_DateKey, Fk_ProjectId, Fk_EmployeesId, Fk_ViewersID, Fk_ServiceKey)
SELECT Distinct Replace( Replace (Replace( Replace (Replace( Replace( Replace( Replace (Replace (Replace(Replace(Replace(Replace(REPLACE(REPLACE(REPLACE(REPLACE(
    SUBSTRING(
        stg.Updates,
        CHARINDEX('Update', stg.Updates) + LEN('Update') + 1,
        CASE
            WHEN CHARINDEX('Service Health RAG', stg.Updates) > 0
            THEN CHARINDEX('Service Health RAG', stg.Updates) - CHARINDEX('Update', stg.Updates) - LEN('Update') - 1
            ELSE LEN(stg.Updates)
        END
    ), '<div>', ''), '</div>', ''), '<br>', ''), '&nbsp;', ''), '<ul>', ' '), '</li>', ''), '</ul>',''),'<li style="list-style-type: disc">', ' '),'<ul style="list-style-type: circle">', ' ' ), '<li style="">', ' '), '<ol>', ' '), '<li>', ' '), '<span>', ' '), '</span>', ' '), '<b>', ''), '</b>', ''), '</ol>', '')
 AS ServiceValue,
    stg.SubmittedTime,
    stg.Updates as UpdatesRichText  ,
    dd.Pk_DateKey as Fk_DateKey,
    dp.Pk_ProjectId AS Fk_ProjectId,
    de.Pk_EmployeeId AS Fk_EmployeesId,
    dv.Pk_EmployeeId AS Fk_ViewersID,
    ds.Pk_ServiceId AS Fk_ServiceKey
FROM
    DMA.StgReports stg
JOIN DMA.DimDate dd ON convert(date, stg.SubmittedTime) = dd.Date
JOIN DMA.DimProject dp ON Replace(Replace(Replace(REPLACE(SUBSTRING(stg.Updates, 1, CHARINDEX('Update', stg.Updates) - 3), 'Service', ''),Char(13),''),Char(10),''),Char(9),'') = dp.ProjectName
JOIN DMA.DimEmployee de ON stg.EmployeeEmail = de.EmployeeEmail 
JOIN DMA.DimEmployee dv ON stg.ViewEmail = dv.EmployeeEmail 
JOIN DMA.DimRag ds ON SUBSTRING(stg.Updates, CHARINDEX('Service Health RAG', stg.Updates) + LEN('Service Health RAG') + 1, LEN(stg.Updates)) = ds.RagName 
WHERE
    stg.Updates LIKE 'Service%' and NOT EXISTS (select 1 from dma.FactReports tf where tf.Updates =  Replace( Replace (Replace( Replace (Replace( Replace( Replace( Replace (Replace (Replace(Replace(Replace(Replace(REPLACE(REPLACE(REPLACE(REPLACE(
    SUBSTRING(
        stg.Updates,
        CHARINDEX('Update', stg.Updates) + LEN('Update') + 1,
        CASE
            WHEN CHARINDEX('Service Health RAG', stg.Updates) > 0
            THEN CHARINDEX('Service Health RAG', stg.Updates) - CHARINDEX('Update', stg.Updates) - LEN('Update') - 1
            ELSE LEN(stg.Updates)
        END
    ), '<div>', ''), '</div>', ''), '<br>', ''), '&nbsp;', ''), '<ul>', ' '), '</li>', ''), '</ul>',''),'<li style="list-style-type: disc">', ' '),'<ul style="list-style-type: circle">', ' ' ), '<li style="">', ' '), '<ol>', ' '), '<li>', ' '), '<span>', ' '), '</span>', ' '), '<b>', ''), '</b>', ''), '</ol>', '')
 
    AND tf.SubmittedTime = stg.SubmittedTime) order by SubmittedTime desc;

-- ----------------------------------------------------------------------------------------------------------------------------

select * from DMA.FactReports where Updates like ' %' or Updates like '% ';

select * from  DMA.FactReports where Updates like '% ';

--update DMA.FactReports
--set Updates = trim(Updates);

select * into #tempF from DMA.FactReports;

truncate table #tempF;


INSERT INTO #tempF (Updates, SubmittedTime,UpdatesRichText, Fk_DateKey, Fk_ProjectId, Fk_EmployeesId, Fk_ViewersID, Fk_ServiceKey)
SELECT Distinct Trim (Replace( Replace (Replace( Replace (Replace( Replace( Replace( Replace (Replace (Replace(Replace(Replace(Replace(REPLACE(REPLACE(REPLACE(REPLACE(
    SUBSTRING(
        stg.Updates,
        CHARINDEX('Update', stg.Updates) + LEN('Update') + 1,
        CASE
            WHEN CHARINDEX('Service Health RAG', stg.Updates) > 0
            THEN CHARINDEX('Service Health RAG', stg.Updates) - CHARINDEX('Update', stg.Updates) - LEN('Update') - 1
            ELSE LEN(stg.Updates)
        END
    ), '<div>', ''), '</div>', ''), '<br>', ''), '&nbsp;', ''), '<ul>', ' '), '</li>', ''), '</ul>',''),'<li style="list-style-type: disc">', ' '),'<ul style="list-style-type: circle">', ' ' ), '<li style="">', ' '), '<ol>', ' '), '<li>', ' '), '<span>', ' '), '</span>', ' '), '<b>', ''), '</b>', ''), '</ol>', ''))
 AS ServiceValue,
    stg.SubmittedTime,
    stg.Updates as UpdatesRichText  ,
    dd.Pk_DateKey as Fk_DateKey,
    dp.Pk_ProjectId AS Fk_ProjectId,
    de.Pk_EmployeeId AS Fk_EmployeesId,
    dv.Pk_EmployeeId AS Fk_ViewersID,
    ds.Pk_ServiceId AS Fk_ServiceKey
FROM
    DMA.StgReports stg
JOIN DMA.DimDate dd ON convert(date, stg.SubmittedTime) = dd.Date
JOIN DMA.DimProject dp ON Replace(Replace(Replace(REPLACE(SUBSTRING(stg.Updates, 1, CHARINDEX('Update', stg.Updates) - 3), 'Service', ''),Char(13),''),Char(10),''),Char(9),'') = dp.ProjectName
JOIN DMA.DimEmployee de ON stg.EmployeeEmail = de.EmployeeEmail 
JOIN DMA.DimEmployee dv ON stg.ViewEmail = dv.EmployeeEmail 
JOIN DMA.DimRag ds ON SUBSTRING(stg.Updates, CHARINDEX('Service Health RAG', stg.Updates) + LEN('Service Health RAG') + 1, LEN(stg.Updates)) = ds.RagName 
WHERE
    stg.Updates LIKE 'Service%' and NOT EXISTS (select 1 from #tempF tf where tf.Updates = Trim( Replace( Replace (Replace( Replace (Replace( Replace( Replace( Replace (Replace (Replace(Replace(Replace(Replace(REPLACE(REPLACE(REPLACE(REPLACE(
    SUBSTRING(
        stg.Updates,
        CHARINDEX('Update', stg.Updates) + LEN('Update') + 1,
        CASE
            WHEN CHARINDEX('Service Health RAG', stg.Updates) > 0
            THEN CHARINDEX('Service Health RAG', stg.Updates) - CHARINDEX('Update', stg.Updates) - LEN('Update') - 1
            ELSE LEN(stg.Updates)
        END
    ), '<div>', ''), '</div>', ''), '<br>', ''), '&nbsp;', ''), '<ul>', ' '), '</li>', ''), '</ul>',''),'<li style="list-style-type: disc">', ' '),'<ul style="list-style-type: circle">', ' ' ), '<li style="">', ' '), '<ol>', ' '), '<li>', ' '), '<span>', ' '), '</span>', ' '), '<b>', ''), '</b>', ''), '</ol>', ''))
 
    AND tf.SubmittedTime = stg.SubmittedTime) order by SubmittedTime desc;


select * from #tempF;

update DMA.FactReports 
set Updates = replace(replace(replace(ProjectName,Char(13),''),Char(10),''),Char(9),'')


-- ----------------------------------------------------- checks ------------------------------------------------------------------

select pk_employeeid as allemps from DMA.DimEmployee;

select * from DMA.DimEmployee;

select Pk_ProjectId as selectall from DMA.DimProject ;

select * from DMA.DimRag ; 

select RagName as selectall from DMA.DimRag;

select ProjectName from DMA.DimProject;

select * from DMA.FactReports order by SubmittedTime desc;

select * from DMA.FactReports where Fk_EmployeesId = 22 order by SubmittedTime desc;

select max(SubmittedTime) as end_date from DMA.FactReports;

--with projectnames as (
--	select Distinct ProjectName from DMA.DimProject

--)select Pk_ProjectId, ProjectName from DMA.DimProject as sourcetable 

select * from ( 
select Pk_ProjectId, ProjectName from DMA.DimProject
) as sourcetable
pivot (
	max(Pk_ProjectId) for ProjectName in ([Beazley / All s], [FDM - All s], [FDM / Small Change], [FDM / Support], [IFRS17 - All Workstreams], [IFRS17 / BR1], [IFRS17 / BR2], [IFRS17 / DevOps], [IFRS17 / Reporting], [SourceBreaker])
) as pivot_table


--with NumberedProjects as (

--	select projectname , row_number() over(order by projectname) as rn from DMA.DimProject

--)

--select max(case when rn =1 then projectname end) as column1,
--max(case when rn =2 then projectname end) as column2,
--max(case when rn =3 then projectname end) as column3,
--max(case when rn =4 then projectname end) as column4
--from NumberedProjects 
--group by ceiling( rn / 3);


create view dma.VProjectName 
as

select ProjectName,  row_number() over(order by Pk_ProjectId) as rn from DMA.DimProject;

select * from dma.VProjectName;

select ProjectName, rn%3 as rownum, row_number() over(partition by rn %3 order by ProjectName)  from dma.VProjectName;

-- drop view dma.VPivotProjects;

create view dma.VPivotProjects
as
 
 select 
 [0] [ProjectName1], [1] [ProjectName2] , [2] [col3]
 from (
	select ProjectName, rn%3 as rownum, row_number() over(partition by rn %3 order by ProjectName) rn2 from dma.VProjectName
 )as sourcetable 

 pivot (
	max(ProjectName) for rownum in ([0], [1], [2])
 )as pivottable;

  select * from DMA.DimProject;

 select * from dma.VPivotProjects;



