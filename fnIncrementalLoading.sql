CREATE TABLE dev.Sales (
    Id INT  Identity (1,1) PRIMARY KEY,
    ProductName VARCHAR(255),
    Quantity INT,
    Amount DECIMAL(10, 2),
    TransactionDate DATE
);

-- Insert sample data
INSERT INTO dev.Sales (ProductName, Quantity, Amount, TransactionDate) VALUES
( 'Laptop', 2, 1500.00, '2024-01-16'),
( 'Smartphone', 5, 750.00, '2024-01-16'),
( 'Headphones', 3, 120.00, '2024-01-15'),
( 'Tablet', 1, 400.00, '2024-01-14'),
( 'Camera', 1, 800.00, '2024-01-13'),
( 'Printer', 2, 300.00, '2024-01-12'),
( 'Desk Chair', 4, 250.00, '2024-01-11'),
( 'Monitor', 1, 600.00, '2024-01-10'),
( 'External HDD', 3, 180.00, '2024-01-09'),
( 'Wireless Mouse', 2, 30.00, '2024-01-08'),
( 'Keyboard', 1, 50.00, '2024-01-07'),
( 'Backpack', 2, 70.00, '2024-01-06'),
( 'Desk Lamp', 1, 25.00, '2024-01-05'),
( 'Coffee Maker', 1, 100.00, '2024-01-04'),
( 'Smartwatch', 3, 180.00, '2024-01-03');



INSERT INTO dev.Sales (ProductName, Quantity, Amount, TransactionDate) VALUES
( 'Laptop', 2, 1500.00, '2024-01-30'),
( 'Smartphone', 5, 750.00, '2024-01-29'),
( 'Headphones', 3, 120.00, '2024-01-29'),
( 'Tablet', 1, 400.00, '2024-01-30');

-- create one more table called Transactions

create table dev.OnlineTransactions (
TransactionId int identity,
TransactionAmount DECIMAL(10, 2),
DateOfTransaction Date

);


insert into dev.OnlineTransactions (TransactionAmount, DateOfTransaction) values 
( 600.00, '2024-01-10'),
(  180.00, '2024-01-09'),
(  30.00, '2024-01-08'),
(  50.00, '2024-01-07'),
(  70.00, '2024-01-06'),
(  25.00, '2024-01-05'),
(  100.00, '2024-01-04'),
(  180.00, '2024-01-03');


insert into dev.OnlineTransactions (TransactionAmount, DateOfTransaction) values 
( 600.00, '2024-01-30'),
(  180.00, '2024-01-29'),
(  30.00, '2024-01-30');

-- create config Table 

-- Drop table  dev.TConfigIncremental

create table dev.TConfigIncremental (
Id int identity,
SchemaName nvarchar(max),
TableName nvarchar(max),
WaterMark date,
IncrementalColumnName nvarchar(max)
);

insert into dev.TConfigIncremental (SchemaName, TableName, WaterMark, IncrementalColumnName) values 
('dev', 'Sales', '2024-01-01', 'TransactionDate');

select * from dev.Sales;

select * from dev.OnlineTransactions;

select * from dev.TConfigIncremental;

-- DROP PROCEDURE dev.UpdateWaterMark;

create procedure dev.UpdateWaterMark (
@SrcTableName nvarchar(max),
@IncrementalColumn nvarchar(max),
@tname nvarchar(max)
)
as 
Begin 

DECLARE @setMax nvarchar(max) = 'select @maxvalue = max(' + @IncrementalColumn + ') from ' + @SrcTableName;
Declare @MaxDate date ;

EXECUTE sp_executesql @setMax, N'@maxvalue date output', @maxvalue = @MaxDate output;

Update dev.TConfigIncremental
set WaterMark = @MaxDate

where TableName = @tname

end


--EXEC dev.UpdateWaterMark 
--    @SrcTableName = 'dev.OnlineTransactions', 
--    @IncrementalColumn = 'DateOfTransaction';


select * from dev.TConfigIncremental;


--DECLARE @tname VARCHAR(50) = 'dev.OnlineTransactions';
--DECLARE @col VARCHAR(50) = 'DateOfTransaction';
--Declare @t varchar(50) = 'OnlineTransactions'
--DECLARE @query NVARCHAR(MAX) = 'SELECT @maxvalue = MAX(' + @col + ') FROM ' + @tname;  
--DECLARE @result DATE;

--EXEC sp_executesql @query, N'@maxvalue DATE OUTPUT', @maxvalue = @result OUTPUT; 

--UPDATE dev.TConfigIncremental
--SET WaterMark = @result
--WHERE TableName = @t;



--select @result;

--SELECT SQL_VARIANT_PROPERTY(@result,'BaseType') BaseType


select * from dev.logs_source_stage;

-- Drop table dev.LogsForIncremental;

create table dev.LogsForIncremental (
LogId int identity(1,1),
DataFactoryName nvarchar(max),
PipelineName nvarchar(max),
SourceTableName nvarchar(max),
DestinationFolderName nvarchar(max),
TriggerTime datetime,
RowsRead int,
RowsCopied int,
CopyDuration int,
WaterMarkDateOfCopied nvarchar(max),
Status nvarchar(max)
);

Drop procedure USPIncrementalLogs;

create procedure dev.USPIncrementalLogs (
@DataFactoryName nvarchar(max),
@PipelineName nvarchar(max),
@SourceTableName nvarchar(max),
@DestinationFolderName nvarchar(max),
@TriggerTime datetime,
@RowsRead int,
@RowsCopied int,
@CopyDuration int,
@WaterMarkDateOfCopied nvarchar(max),
@status nvarchar(max)

)

as
Begin
	Insert into dev.LogsForIncremental (DataFactoryName,PipelineName, SourceTableName, DestinationFolderName,TriggerTime,RowsRead,RowsCopied,CopyDuration,WaterMarkDateOfCopied, Status)
	values (
		@DataFactoryName,
		@PipelineName,
		@SourceTableName,
		@DestinationFolderName,
		@TriggerTime,
		@RowsRead,
		@RowsCopied,
		@CopyDuration,
		@WaterMarkDateOfCopied,
		@status
	)

End;


select * from dev.LogsForIncremental;

select * from dev.TConfigIncremental;

select * from dev.config_stg_raw;


SELECT Column_name FROM ProspectManagement.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'config_stg_raw';



SELECT DATEPART(month, '2017/08/25') AS DatePartInt;

Select DATEPART(ww, '2024/01/30') AS DatePartInt; -- weekday 0,1...6

Select DATEPART(dw, '2024/01/30') AS DatePartInt