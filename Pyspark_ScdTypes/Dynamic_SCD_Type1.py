# Databricks notebook source
# MAGIC %sql
# MAGIC select * from dev.tcontrolscd1

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *

# Get current timestamp
today_date = datetime.now()

# Extract date components
year = today_date.year
month = today_date.month
day = today_date.day

Paths = [
    f"dbfs:/Project/Silver/EmployeeLoginDetails/{year}/{month}/{day}",
    f"dbfs:/Project/Silver/Xyenta_Leaves_2022/{year}/{month}/{day}",
    f"dbfs:/Project/Silver/XyentaHolidays/{year}/{month}/{day}"
]
control_df = spark.sql("select * from dev.tcontrolscd1")
DeltaTables=["dev.dimdesignation","dev.dimholiday","dev.dimleavetype"]
def scdtype1(paths, control_df, target_table_name):
    try:
        dim_df = spark.sql(f"SELECT * FROM {target_table_name}")
        dim_df.show()
        tgt_table = control_df.filter(col('TargetTableName') == target_table_name)
        tgt_cols_row = tgt_table.filter(col('IsAutoKey') != 'Y').select('TargetColumnNames').collect()
        tgt_cols = [col_name.TargetColumnNames for col_name in tgt_cols_row]  
        tgt_table_name = tgt_table.select('TargetTableName').distinct().collect()[0][0]
        print("Target Table Name:", tgt_table_name)
        print("Target Column Names:")
        print(tgt_cols)
        src_table_name = tgt_table.filter(col('SourceTableName') != '').select('SourceTableName').distinct()
        src_table_name = src_table_name.collect()[0][0]
        print("SourceTableName:", src_table_name)
        scd_columns = tgt_table.filter(col("IsScd") == "Y").selectExpr("concat(TargetTableName, '.', TargetColumnNames, ' == x.', SourceColumnNames) AS scd_cols")
        scd_columns.show(truncate=False)
        logic_cols = tgt_table.filter(col("IsLogicColumn") == "Y").selectExpr("concat('y', '.', TargetColumnNames, ' == x.', AliasColumnNames) AS logic_col")
        logic_cols.show(truncate=False)
        logic_col_name = tgt_table.filter(col("IsLogicColumn") == "Y").select("TargetColumnNames").collect()[0][0]
        print(logic_col_name)
        logic_list = logic_cols.selectExpr("concat_ws(' AND ', collect_list(logic_col)) AS logic_col")
        logic_col_list = logic_list.collect()[0][0]
        print('logic_col_list : ', logic_col_list)
        print("AutoKey columnNames:")
        auto_col = tgt_table.filter(col('IsAutoKey') == 'Y').select(col('TargetColumnNames')).collect()[0][0]
        print(auto_col)
        LoadType = tgt_table.filter(col("LoadType") == "SCD1").select("LoadType").distinct()
        print("LoadType")
        LoadType.show()
        for path in paths:
            sourceDF = spark.read.parquet(path)
            Source_table_name = path.split("/")[-4]
            if Source_table_name == src_table_name and LoadType.select("LoadType").collect()[0][0] == 'SCD1':
                stg_df = sourceDF.select(*tgt_cols).distinct() 
                display(stg_df)
                print("check for new records...")
                logic_col_list_expr = expr(logic_col_list)
                drop_columns_expr = expr(auto_col)
                print(logic_col_list_expr)
                new_records = stg_df.alias('x').join(dim_df.alias('y'), logic_col_list_expr, 'left_anti')
                print("new records....")
                new_records.show()
                updated_records = stg_df.alias('x').join(dim_df.alias('Y'), logic_col_list_expr, 'inner').select(dim_df['*']).drop(drop_columns_expr)
                print("matching from src to target ")
                updated_records.show()
                combined_records = new_records.unionByName(updated_records)
                for col_name in combined_records.columns:
                    combined_records = combined_records.filter(col(col_name)!='unknown').distinct()
                    # Show the filtered DataFrame
                    combined_records.show()
                    combined_records.write.mode('overwrite').saveAsTable(f"{tgt_table_name}")
                    table_path = f"dbfs:/Project/Gold/{tgt_table_name}/{year}/{month}/{day}"
                    combined_records.write.format("delta").mode("overwrite").save(table_path)
                    print("done")
    except Exception as e:
        print("An error occurred:", str(e))

for tables in DeltaTables:
    scdtype1(Paths, control_df, tables)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.dimholiday

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.dimdesignation

# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from dev.dimleavetype
