
select name from sys.schemas;
-- DimEmployees creation
Drop table DMA.DimEmployee;

create table DMA.DimEmployee(
Pk_EmployeeId int identity(1,1) Primary key,
EmployeeFirstName nvarchar(max),
EmployeeLastName nvarchar(max),
EmployeeEmail nvarchar(max),
CreatedDate datetime DEFAULT getDate()
);

select * from DMA.DimEmployee;


-- DateDimension creation 
Drop table DMA.DimDate;

create table DMA.DimDate(
Pk_DateKey int  Primary key,
Date date,
DayofWeek nvarchar(max),
DayofMonth nvarchar(max),
MonthName nvarchar(max),
Quatr nvarchar(max),
Year int,
CreatedDate datetime DEFAULT getDate()
);

select * from DMA.DimDate;

-- FactReports creation 
Drop table DMA.FactReports;

create table DMA.FactReports(
Pk_Id int identity(1,1) Primary key,
Updates nvarchar(max),
SubmittedTime datetime,
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

select * from DMA.FactReports;

-- checks for PK and FK --
SELECT * FROM information_schema.key_column_usage

-- create a triggers for updatedAt values in Dim and Fact Tables
--create trigger UpdatedAtDimEmpF on DMA.DimEmployee
--after update as
--Begin
--update DMA.DimEmployee set updatedAt = getdate()
--where Pk_EmployeeId in (select Pk_EmployeeId from inserted)
--end;

---- 
--create trigger UpdatedAtDimDate2 on DMA.DimDate
--after update as
--Begin
--update DMA.DimDate set updatedAt = getdate()
--where Pk_DateKey in (select Pk_DateKey from inserted)
--end;



---- create a triggers for Fact to updateAt column
--create trigger UpdatedAtFactReport on DMA.FactReports
--after update as
--Begin
--update DMA.FactReports set updatedAt = getdate()
--where Pk_id in (select Pk_id from inserted)
--end;

-- Populate DimDate Table upto 3months from now

SELECT CONVERT(date, GETDATE()); -- Date

select day(GetDate()); --DayofMonth

SELECT DATENAME(Week, getdate()); --WeekNumber of year

select DATENAME(Weekday, getdate()); -- Weekday Name

select year(getDate());

-- truncate table DMA.DimDate;

-- --------------------------------------------------------
--with t as (
--	select 1 as sr_no
--	union all
--	select sr_no+1 from t where sr_no < 90
--),
--t1 as (
--	select DateAdd(Day, sr_no-1,getdate()) as a from t
--)
--Insert into DMA.DimDate(DateKey,Date, DayOfWeek, DayOfMonth, MonthName, Quatr, Year)
--select replace(convert(date, a), '-', '') as DateKey, convert(date, a) as Date, DATENAME(Weekday, a) as DayOfWeek, 
--day(a) as DayOfMonth, DATENAME(MONTH, a) AS MonthName,  'Q' + CAST(DATEPART(QUARTER, a) AS NVARCHAR(MAX)) AS Quatr,
--YEAR(a) AS Year from t1;

-- 
select * from DMA.DimDate;

select * from dev.Logs_Stage_raw;


-- truncate table DMA.DimEmployees;

select * from DMA.DimEmployees;
-- when ever table ref as fk constraint in another table .. instead of truncate use below query to Truncate the rows
DELETE FROM DMA.DimEmployees;

Delete from DMA.FactReports;

-- DELETE FROM DMA.DimDate;

select * from DMA.DimDate;

-- truncate table DMA.FactReports;

select * from DMA.FactReports;

-- DROP TRIGGER [IF EXISTS] [schema_name.]trigger1, trigger2, ... ];

DROP TRIGGER UpdatedAtDimEmpF;

SELECT  
    name,
    is_instead_of_trigger
FROM 
    sys.triggers  
WHERE 
    type = 'TR'

-- All checks -----------------
select * from DMA.TControlSourceTargetMapping;

select * from DMA.LogsSourceStage;

select * from DMA.DimEmployees; -- 15 0

select * from DMA.DimDate ; --122

select * from DMA.FactReports; -- 15 0 

-- create 3 more dimensions Project, Client, Services

Drop table DMA.DimClient

create table DMA.DimClient(
Pk_ClientId int identity(1,1) primary key,
ClientName nvarchar(max),
CreatedDate datetime DEFAULT getDate()

);

-- create trigger updateAt fro DimClient
--create trigger UpdatedAtDimClient on DMA.DimClient
--after update as
--Begin
--update DMA.DimClient set updatedAt = getdate()
--where Pk_ClientId in (select Pk_ClientId from inserted)
--end;

-- ----------------------------------------------------------
Drop Table DMA.DimProject;

create table DMA.DimProject(
Pk_ProjectId int identity(1,1) Primary key,
ProjectName nvarchar(max),
Fk_ClientId int,
CreatedDate datetime DEFAULT getDate(),
FOREIGN KEY (Fk_ClientId) REFERENCES DMA.DimClient(Pk_ClientId),
);

-- create trigger updateAt fro DimProject
--create trigger UpdatedAtDimProject on DMA.DimProject
--after update as
--Begin
--update DMA.DimProject set updatedAt = getdate()
--where Pk_ProjectId in (select Pk_ProjectId from inserted)
--end;
--  --------------------------------------------
Drop table DMA.DimRag;

create table DMA.DimRag(
Pk_ServiceId int identity(1,1) primary key ,
RagName nvarchar(max),
Description nvarchar(max),
CreatedDate datetime DEFAULT getDate()
);

-- create trigger updateAt fro DimProject
--create trigger UpdatedAtDimService on DMA.DimRag
--after update as
--Begin
--update DMA.DimRag set updatedAt = getdate()
--where Pk_ServiceId in (select Pk_ServiceId from inserted)
--end;

-- ------------------------------------------------------------------
select * from DMA.DimClient;

select count(*) from DMA.DimProject; -- 6 0 

select count(*) from DMA.DimRag; -- 2 0 

select count(*) from DMA.DimEmployee; -- 16 0

select * from DMA.DimEmployee;

select * from DMA.DimDate ; --122

select * from DMA.FactReports; --  33

select * from DMA.DimRag;

select * from DMA.DimProject;




select a.ProjectName, a.Id, b.DEmployeesId, b.DProjectId, c.EmployeeId, c.EmployeeEmail from DMA.DimProject a
join DMA.FactReports b on b.DProjectId = a.Id
join DMA.DimEmployees c on b.DEmployeesId = c.EmployeeId  ;

-- -- --------------------------------------------------------
select * into #tempS from DMA.DimService;

SELECT * FROM DMA.DimService
-- ------------------------------------------------------------
UPDATE DMA.DimRag
SET Description = case 
				  when RagName = 'Green' then 'Good, expectations are being met.'
				  else Description
				  END;

insert into DMA.DimService (RagName, Description) values ('Amber', 'requires improvement, expectations not being met.'),
('Red' , 'Poor, requires intervention');

-- --- ---------------------------------------------------------
select substring('lokeswar', 2, len('lokeswar') ); -- okeswar

select concat(upper(substring('lokeswar',1,1)), lower(substring('lokeswar',2, len('lokeswar'))));

-- -------------------------------------------
select * into #tempE from DMA.DimEmployee;


WITH UniqueViewEmails AS (
    SELECT DISTINCT ViewEmail
    FROM DMA.StgReports
)

-- INSERT INTO #tempE (EmployeeFirstName, EmployeeLastName, EmployeeEmail)
SELECT
    -- PARSENAME(s.ViewEmail, 3) AS EmployeeFirstName,
	CONCAT(UPPER(SUBSTRING(PARSENAME(s.ViewEmail, 3), 1, 1)), LOWER(SUBSTRING(PARSENAME(s.ViewEmail, 3), 2, LEN(PARSENAME(s.ViewEmail, 3))))) AS EmployeeFirstName,

    REPLACE(PARSENAME(REPLACE(s.ViewEmail, '@', '.'), 3), '.', '') AS EmployeeLastName,
    s.ViewEmail AS EmployeeEmail
FROM
    UniqueViewEmails s
WHERE
    NOT EXISTS (
        SELECT 1
        FROM DMA.DimEmployee t
        WHERE t.EmployeeEmail = s.ViewEmail
);

update DMA.DimEmployee
set EmployeeFirstName = concat(upper(substring(EmployeeFirstName,1,1)), lower(substring(EmployeeFirstName,2, len(EmployeeFirstName)))),

	EmployeeLastName = concat(upper(substring(EmployeeLastName,1,1)), lower(substring(EmployeeLastName,2, len(EmployeeLastName))));




-- 
UPDATE DMA.DimService
SET RagName = CASE
  WHEN RagName = 'None1' THEN 'None11'
  ELSE RagName
END;
