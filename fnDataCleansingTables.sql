select * from dev.config_src_stg;

select * from dev.config_stg_raw;

--update dev.config_src_stg
--set src_container = 'xyenta-src',
--destination_container = 'xyenta-stg';


select * into dev.TControlSourceToStage from dev.config_src_stg;

select * from dev.TControlSourceToStage;

update dev.TControlSourceToStage
set src_container = 'xyenta-dev-96-src',
destination_container = 'xyenta-dev-96-stg';


select * into dev.TControlStageToRaw from dev.config_stg_raw;

select * from dev.TControlStageToRaw;

update dev.TControlStageToRaw
set src_container = 'xyenta-dev-96-stg',
destination_container = 'xyenta-dev-96-raw';


select * from dev.Logs_Source_Stage;

truncate table dev.TControlDataValidation;

-- drop table dev.TControlDataValidation;

create table Dev.TControlDataValidation(
Id int identity(1,1),
FileName nvarchar(max),
FilePath nvarchar(max),
StgFolder nvarchar(max),
OutFolderName nvarchar(max),
CreatedAt datetime DEFAULT getDate()
);

DECLARE @dynamicDate VARCHAR(10) = format(convert(Date, DATEADD(day, -4, GETDATE())), 'yyyy/MM/dd')

-- select * from dev.TControlDataValidation;

-- dbfs:/mnt/Orders/
-- dbfs:/mnt/People/
-- dbfs:/mnt/Returns/
-- DECLARE @dynamicDate VARCHAR(10) = format(convert(Date,  GETDATE()), 'yyyy/MM/dd')
--Declare @urlRawOrders nvarchar(max) ; 
--set @urlRawOrders = 'dbfs:/mnt/raw/Orders/'
--Declare @urlRawPeople nvarchar(max);
--set @urlRawPeople = 'dbfs:/mnt/raw/People/' 
--Declare @urlRawReturns nvarchar(max);
--set @urlRawReturns = 'dbfs:/mnt/raw/Returns/' 

-- select @dynamicDate, @urlRawReturns;

insert into dev.TControlDataValidation (FileName, FilePath,StgFolder , OutFolderName) values ('Orders_data', 'dbfs:/mnt/StageLayer/Orders', 'Orders', 'Orders'),
('People_data', 'dbfs:/mnt/StageLayer/People','People', 'People'), ('Returns_data', 'dbfs:/mnt/StageLayer/Returns', 'Returns', 'Returns');


select * from dev.TControlDataValidation;

-- delete dev.TControlDataValidation where Id = 4;


select * from dev.schema_refe;


-- dummy

select * into dev.TDataGovernance from dev.TControlDataValidation;

truncate  table dev.TDataGovernance;

insert into dev.TDataGovernance (FileName, FilePath, OutPath, OutFolderName) values ('Orders_data', 'dbfs:/mnt/stg/Orders/', 'dbfs:/mnt/RawLayer', 'Orders'),
('People_data', 'dbfs:/mnt/stg/People/','dbfs:/mnt/RawLayer', 'People'), ('Returns_data', 'dbfs:/mnt/stg/Returns/', 'dbfs:/mnt/RawLayer', 'Returns'),
('', '', 'dbfs:/mnt/RawLayer', 'BadRecords');

select * from dev.TDataGovernance;

-- delete dev.TDataGovernance where Id = 4;






-- ---------------------------------------------------------------------------------------------------------
--select * from Dev.schema_refe;

---- Drop table Dev.TControlDataValidation;

--create table Dev.TControlDataValidation(
--Id int identity(1,1),
--FileName nvarchar(max),
--FilePath nvarchar(max),
--OutPath nvarchar(max),
--OutFolderName nvarchar(max),
--CreatedAt datetime DEFAULT getDate()
--);


---- Declare the url which takes date dynamically

--SELECT DATEDIFF(year, '2017/08/25', '2011/08/25') AS DateDiff;

--select format(dateadd(day, -10, getdate()), 'yyyy/MM/dd'); -- 2023/12/16


--SELECT convert(Date, DATEADD(day, -4, GETDATE()));

--select format(convert(Date, DATEADD(day, -4, GETDATE())), 'yyyy/MM/dd');
---- -----------------------------------------------------------------------------------

--DECLARE @dynamicDate VARCHAR(10) = format(convert(Date, DATEADD(day, -4, GETDATE())), 'yyyy/MM/dd'); -- Replace with your actual dynamic date

---- Construct the URL
--DECLARE @urlOrders VARCHAR(500);
--SET @urlOrders = 'abfss://xyenta-stg@adlsxyentadev.dfs.core.windows.net/Orders/' + @dynamicDate + '/Orders_data.csv';

---- DECLARE @dynamicDate VARCHAR(10) = convert(Date, DATEADD(day, -4, GETDATE())); -- Replace with your actual dynamic date

--DECLARE @urlReturns VARCHAR(500);
--SET @urlReturns = 'abfss://xyenta-stg@adlsxyentadev.dfs.core.windows.net/Returns/' + @dynamicDate + '/Returns_data.csv';


--insert into Dev.TControlDataValidation(TargetDF, FileName, FilePath, RefDF) values 
--('Returns_df', 'Returns_data',@urlReturns, 'reference_df'),
--('Orders_df', 'Orders_data', @urlOrders, 'reference_df');


---- checks
--select * from Dev.TControlDataValidation;

---- truncate table Dev.TControlDataValidation;

--select case 
--when GETDATE () = format(getdate(), 'yyyy/MM/dd') then 'Pass' else 'Fail'
--end 



--select * from dev.schema_refe;


CREATE TABLE dev.products (category VARCHAR(50),brand VARCHAR(50));

INSERT INTO dev.products (category, brand) VALUES('Chocolate', 'KITKAT');
INSERT INTO dev.products (category, brand) VALUES(NULL, 'Dairy Milk');
INSERT INTO dev.products (category, brand) VALUES(NULL, '5 Star');
INSERT INTO dev.products (category, brand) VALUES(NULL, 'Milkybar');
INSERT INTO dev.products (category, brand) VALUES(NULL, 'Kissme');
INSERT INTO dev.products (category, brand) VALUES('Biscuit', 'Oreo');
INSERT INTO dev.products (category, brand) VALUES(NULL, 'Parle');
INSERT INTO dev.products (category, brand) VALUES(NULL, 'Snack');
INSERT INTO dev.products (category, brand) VALUES(NULL, 'fifty fifty');
INSERT INTO dev.products (category, brand) VALUES(NULL, 'Bar');


select * from dev.products;


with cte as (

select *, row_number() over(order by  (select 1)) as rn from dev.products

), cte1 as (

	select *, count(category) over(order by rn) as cc from cte
)

select category , FIRST_VALUE(category) over(partition by cc order by cc) as updatedCategory , brand from cte1;