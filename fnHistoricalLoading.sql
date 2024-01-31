create table dev.HistoricalSales (
Sales_Id int identity(1,1) primary key,
Quantity int,
Price int,
Sales_date datetime

);




insert into dev.HistoricalSales (Quantity, Price, Sales_date) values (2, 50, '2023-12-20 12:43:43'),

(3, 60, '2023-12-20 11:43:43'),

(1, 50, '2023-12-21 10:43:43'),

(2, 50, '2023-12-21 12:43:43'),

(5, 50, '2023-12-29 12:43:43');





create table dev.SalesDim (
SalesKey int identity(1,1) primary key,
Sales_Id int,
Quantity int,
Price int,
Sales_date datetime

);



-- Drop table dev.TConfigHistoricalLoad;

create table dev.TConfigHistoricalLoad (
Id int identity(1,1) ,

SchemaName varchar(10),

SourceTableName varchar(55),

SourceColumnName varchar(55),

TargetSchema varchar(10),

TargetTableName varchar(55)

);

insert into dev.TConfigHistoricalLoad (SchemaName, SourceTableName, SourceColumnName,TargetSchema, TargetTableName) values ('dev', 'HistoricalSales','Sales_date' ,'dev', 'SalesDim');


select * from dev.TConfigHistoricalLoad;

select * from dev.SalesDim;

select * from dev.HistoricalSales;

-- Drop procedure dev.SprocHistoricalLoad;

create procedure dev.SprocHistoricalLoad (
@startDate nvarchar(100),
@endDate nvarchar(100),
@tableName nvarchar(100),
@colName nvarchar(100)

)

as

Begin

	Declare @sqlQuery Nvarchar(max)
	set @sqlQuery = 'select * from ' + @tableName + ' where ' + @colName + ' >= ''' + @startDate + ''' and ' + @colName + ' <= ''' + @endDate + ''''

	EXECUTE sp_executesql @sqlQuery 
End


-- Execute dev.SprocHistoricalLoad @startDate = '2023-12-20 12:43:43' , @endDate = '2023-12-21 12:43:43' , @tableName = 'dev.HistoricalSales' , @colName = 'Sales_date'

--Declare @sd nvarchar(100) = '2023-12-20 12:43:43';
--Declare @ed nvarchar(100) = '2023-12-21 12:43:43';
--Declare @t nvarchar(100) = 'dev.HistoricalSales';
--Declare @c nvarchar(100) = 'Sales_date';

--Declare @qry Nvarchar(max);

--set @qry = 'select * from ' + @t + ' where ' + @c + ' > ''' + @sd + ''' and ' + @c + ' <= ''' + @ed + ''''

---- set @qry = 'select * from ' + @t + ' where ' + @c + ' > ' + @sd + ' and ' + @c + ' <= ' + @ed  select * from dev.HistoricalSales where Sales_date > 2023-12-20 12:43:43 and Sales_date <= 2023-12-21 12:43:43

--select @qry;

--EXECUTE sp_executesql @qry;


-- Drop table  dev.LogsForHistoricalLoad;

create table dev.LogsForHistoricalLoad (
LogId int identity(1,1),
DataFactoryName nvarchar(max),
PipelineName nvarchar(max),
SourceTableName nvarchar(max),
DestinationTableName nvarchar(max),
TriggerTime datetime,
TriggerType nvarchar(max),
RowsRead int,
RowsCopied int,
CopyDuration int,
DeclaredStartDate nvarchar(max),
DeclaredEndDate nvarchar(max),
Status nvarchar(max)
);



-- Drop procedure dev.USPHistoricalLogs;

create procedure dev.USPHistoricalLogs (
@DataFactoryName nvarchar(max),
@PipelineName nvarchar(max),
@SourceTableName nvarchar(max),
@DestinationTableName nvarchar(max),
@TriggerTime datetime,
@TriggerType nvarchar(max),
@RowsRead int,
@RowsCopied int,
@CopyDuration int,
@DeclaredStartDate nvarchar(max),
@DeclaredEndDate nvarchar(max),
@status nvarchar(max)

)

as
Begin
	Insert into dev.LogsForHistoricalLoad(DataFactoryName,PipelineName, SourceTableName, DestinationTableName,TriggerTime,TriggerType,RowsRead,RowsCopied,CopyDuration,DeclaredStartDate,DeclaredEndDate, Status)
	values (
		@DataFactoryName,
		@PipelineName,
		@SourceTableName,
		@DestinationTableName,
		@TriggerTime,
		@TriggerType,
		@RowsRead,
		@RowsCopied,
		@CopyDuration,
		@DeclaredStartDate,
		@DeclaredEndDate,
		@status
	)

End;


select * from dev.HistoricalSales;

-- truncate table dev.SalesDim;

select * from dev.SalesDim;

select * from dev.TConfigHistoricalLoad;

select * from dev.LogsForHistoricalLoad;