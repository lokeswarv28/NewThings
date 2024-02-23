
select * from dev.DimEmployees;

-- truncate table dev.DimEmployees;

select * from dev.TControlSCD;

Declare @final_stmt Nvarchar(max);

with t as 
(select * from dev.TControlSCD where TargetTableName = 'dev.DimEmployees'),

tgt_cols as (select * from t where IsAutoGeneration != 'Y'),
tgt_cols_list as (select STRING_AGG(TargetTableName, ',') as ins_cols from tgt_cols),
srctable as (select distinct SourceTableName from t where SourceTableName != ''),
src_cols as (select * from t where SourceColumnName!= ''),
src_cols_list as (select STRING_AGG(SourceTableName +'.'+ SourceColumnName, ', ') +' ,' + ''''+CONVERT(varchar, GETDATE(),20) + ''''+ ', CAST(''9999/12/31 00:00:00'' AS DATE)' as ins_src_cols
from src_cols),
tgt_table as (select distinct TargetTableName from t ),
src_qry as (select  SourceTableName, SourceTableName+'.'+SourceColumnName as cols from t where SourceColumnName != ''),
src_cols_insert as (select STRING_AGG(SourceColumnName, ',') + ' ,' + ''''+CONVERT(varchar, GETDATE(),20) + ''''+ ', CAST(''9999/12/31 00:00:00'' AS DATE)' as ins_src_colsinsert from src_cols ),
src_list as (select 'Select ' + STRING_AGG(cols, ',') + ' From' + SourceTableName as srccols from src_qry group by SourceTableName),
scd_columns as (select distinct TargetTableName +'.'+ TargetColumnName +'= x. '+ SourceColumnName as scd_cols from t where isScdColumn = 'Y'),
scd_list as (select String_agg(scd_cols, ' AND ') as scd_cols from scd_columns),
logic_cols as ( SELECT TargetTableName + '.' + TargetColumnName + '=x.' + SourceColumnName as logic_col  
    FROM t
    WHERE IsLogicalColumn = 'Y'),
logic_list as (select STRING_AGG(logic_col,' AND ') as logic_col from logic_cols),
final_stmt as (
select + 'Update ' + tgt_table.TargetTableName +
' set EffEndDate = '+ '''' + convert(varchar, getdate(), 20) +''''+
'From ' + tgt_table.TargetTableName +
' Inner join ' + srctable.SourceTableName + ' x on ' + logic_list.logic_col +
' where not ('+ scd_list.scd_cols +')' +
' and EffEndDate = CAST(''9999/12/31 00:00:00'' AS DATE); ' as update_qry
from tgt_table, srctable, logic_list, scd_list
)
-- select * from final_stmt;
select @final_stmt = update_qry from final_stmt

-- select @final_stmt;

EXECUTE sp_executesql @final_stmt;

/*
Update dev.DimEmployees set EffEndDate = '2023-12-26 05:54:27'From dev.DimEmployees Inner join dev.StgEmployees x on dev.DimEmployees.EmployeeId=x.EmployeeId where not (dev.DimEmployees.City= x. City AND dev.DimEmployees.EmployeeLastName= x. EmployeeLastName) 
and EffEndDate = CAST('9999/12/31 00:00:00' AS DATE); 
*/


select * from dev.DimEmployees;

update dev.StgEmployees
set EmployeeLastName = 'Jena', City = 'Hyderabad'
where EmployeeId = 104;

select * from dev.TControlDataValidation;

-- abfss://xyenta-stg@adlsxyentadev.dfs.core.windows.net/Returns/2023/12/11/Returns_data.csv