select * from dev.TControlSCD;

Declare @final_stmt_update Nvarchar(max);

with t_Update as 
(select * from dev.TControlSCD where TargetTableName = 'dev.DimStore'),

tgt_cols as (select * from t_Update where IsAutoGeneration != 'Y'),
tgt_cols_list as (select STRING_AGG(TargetTableName, ',') as ins_cols from tgt_cols),
srctable as (select distinct SourceTableName from t_Update where SourceTableName != ''),
src_cols as (select * from t_Update where SourceColumnName!= ''),
src_cols_list as (select STRING_AGG(SourceTableName +'.'+ SourceColumnName, ', ') +' ,' + ''''+CONVERT(varchar, GETDATE(),20) + ''''+ ', CAST(''9999/12/31 00:00:00'' AS DATE)' as ins_src_cols
from src_cols),
tgt_table as (select distinct TargetTableName from t_Update ),
src_qry as (select  SourceTableName, SourceTableName+'.'+SourceColumnName as cols from t_Update where SourceColumnName != ''),
src_cols_insert as (select STRING_AGG(SourceColumnName, ',') + ' ,' + ''''+CONVERT(varchar, GETDATE(),20) + ''''+ ', CAST(''9999/12/31 00:00:00'' AS DATE)' as ins_src_colsinsert from src_cols ),
src_list as (select 'Select ' + STRING_AGG(cols, ',') + ' From' + SourceTableName as srccols from src_qry group by SourceTableName),
scd_columns as (select distinct TargetTableName +'.'+ TargetColumnName +'= x. '+ SourceColumnName as scd_cols from t_Update where isScdColumn = 'Y'),
scd_list as (select String_agg(scd_cols, ' AND ') as scd_cols from scd_columns),
logic_cols as ( SELECT TargetTableName + '.' + TargetColumnName + '=x.' + SourceColumnName as logic_col  
    FROM t_Update
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
select @final_stmt_update = update_qry from final_stmt

-- select @final_stmt;

EXECUTE sp_executesql @final_stmt_update;



-- --------------------------Insert --------------

Declare @final_stmt nvarchar(max);
with t as (select * from dev.TControlSCD where TargetTableName = 'dev.DimStore'),
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


-- --------------------------------------------------------------------------------------

select * from Dev.DimStore;



