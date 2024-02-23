-- select * from dev.StgEmployees;


select * from Dev.TControlSCD;

Declare @final_stmt  nvarchar(max);

with t as (select * from dev.TControlSCD where TargetTableName = 'dev.DimEmployees'),
tgt_table as (select distinct TargetTableName from t),
tgt_cols as (select * from t where IsAutoGeneration != 'Y'),
tgt_cols_list as (select STRING_AGG(TargetColumnName, ',') as ins_cols from tgt_cols),
srctable as (select distinct SourceTableName from t),
src_cols as (select * from t where SourceColumnName != ''),
src_cols_list as (select STRING_AGG(SourceTableName + ' . ' + SourceColumnName, ',') + ' ,' + ''''+CONVERT(varchar, GETDATE(),20) + ''''+ ', CAST(''9999/12/31 00:00:00'' AS DATE)' as ins_src_cols from src_cols),
src_cols_insert_list as (select STRING_AGG(SourceColumnName, ',')+ ' ,' + ''''+CONVERT(varchar, GETDATE(),20) + ''''+ ', CAST(''9999/12/31 00:00:00'' AS DATE)' as ins_src_cols_insert from src_cols ),
src_qry as (select  SourceTableName, SourceTableName+'.'+SourceColumnName as cols from t where SourceColumnName != ''),
src_list as (select 'select ' + STRING_AGG(cols, ',') + ' From ' + SourceTableName as srccols from src_qry group by SourceTableName),
scd_columns as (select distinct TargetTableName+'.'+TargetColumnName +' = x.'+SourceColumnName as scd_cols from t where isScdColumn = 'Y'),
scd_list as (select STRING_AGG(scd_cols, ' AND ') as scd_cols from scd_columns),
logic_cols as (select distinct TargetTableName+ '. '+TargetColumnName+ ' = x.' + sourceColumnName as logic_col from t where IsLogicalColumn = 'Y'),
logic_col_list as (select STRING_AGG(logic_col, 'AND') as logic_col from logic_cols),
final_Stmt as ( select 'Insert into ' + tgt_table.TargetTableName + '(' + tgt_cols_list.ins_cols +')' +
					   ' select ' + src_cols_insert_list.ins_src_cols_insert + ' from ' + srctable.SourceTableName +' x ' +
					   ' where not exists  (' +
					   '    select 1 from ' + tgt_table.TargetTableName + 
					   '    where EffEndDate = CAST(''9999/12/31 00:00:00'' AS DATE)' +
					   '    and (' + logic_col_list.logic_col + ')' +
					   '    and ('+ scd_list.scd_cols +') ' + ');' as insert_qry
					   from tgt_table, tgt_cols_list, src_cols_insert_list, srctable, logic_col_list, scd_list
)
-- select * from final_Stmt;
select @final_stmt = insert_qry from final_Stmt

-- select @final_stmt;

EXECUTE sp_executesql @final_stmt;

/*
Insert into dev.DimEmployees(EmployeeId,EmployeeFirstName,EmployeeLastName,DOB,Gender,City,EffStartDate,EffEndDate) select EmployeeId,EmployeeFirstName,EmployeeLastName,DOB,Gender,City ,'2023-12-26 06:50:20', CAST('9999/12/31 00:00:00' AS DATE) from dev.StgEmployees x  where not exists  (    select 1 from dev.DimEmployees    where EffEndDate = CAST('9999/12/31 00:00:00' AS DATE)    
and (dev.DimEmployees. EmployeeId = x.EmployeeId)    and (dev.DimEmployees.City = x.City AND dev.DimEmployees.EmployeeLastName = x.EmployeeLastName) );
*/

select * from Dev.DimEmployees;
