# Databricks notebook source
# MAGIC %sql
# MAGIC select * from dev.tcontrolscd1

# COMMAND ----------

from delta import * 
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from datetime import *

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
DeltaTables=["dev.dimemployee","dev.dimmanager"]
def scdtype2(paths, control_df, target_table_name):
    try:
        dim_df = spark.sql(f"SELECT * FROM {target_table_name}")
        targetTable = DeltaTable.forPath(spark, f"dbfs:/Project/Gold/{target_table_name}/{year}/{month}/{day}")
        dim_df.show()
        tgt_table = control_df.filter(col('TargetTableName') == target_table_name)
        tgt_cols_row = tgt_table.filter(col('IsAutoKey') != 'Y').select('TargetColumnNames').collect()
        tgt_cols = [col_name.TargetColumnNames for col_name in tgt_cols_row]  # Extracting column names from Row objects
        tgt_table_name = tgt_table.select('TargetTableName').distinct().collect()[0][0]
        print("Target Table Name:", tgt_table_name)
        print("Target Column Names:")
        print(tgt_cols)
        src_cols_row = tgt_table.filter(col('IsAutoKey') != 'Y').select('SourceColumnNames').collect()
        src_cols = [col_name.SourceColumnNames for col_name in src_cols_row]
        print("sourcecolumns")
        print(src_cols)
        alias_columns_row = tgt_table.filter(col("IsAutoKey") != "Y").select('AliasColumnNames').collect()
        print("alias_names")
        print(alias_columns_row)
        src_table_name = tgt_table.filter(col('SourceTableName') != '').select('SourceTableName').distinct()
        src_table_name = src_table_name.collect()[0][0]
        print("SourceTableName:", src_table_name)
        scd_columns = tgt_table.filter(col("IsScd") == "Y").selectExpr(
            f"concat(TargetTableName, '.', TargetColumnNames, ' != x.', SourceColumnNames) AS scd_cols")
        scd_columns.show(truncate=False)
        logic_cols = tgt_table.filter(col("IsLogicColumn") == "Y").selectExpr(
            f"concat('y', '.', TargetColumnNames, ' == x.', SourceColumnNames) AS logic_col")
        logic_cols.show(truncate=False)
        logic_col_name = tgt_table.filter(col("IsLogicColumn") == "Y").select("TargetColumnNames").collect()[0][0]
        print(logic_col_name)
        logic_list = logic_cols.selectExpr("concat_ws(' AND ', collect_list(logic_col)) AS logic_col")
        logic_col_list = logic_list.collect()[0][0]
        print('logic_col_list : ', logic_col_list)
        print("AutoKey columnNames:")
        auto_col = tgt_table.filter(col('IsAutoKey') == 'Y').select('TargetColumnNames').collect()[0][0]
        print(auto_col)
        LoadType = tgt_table.filter(col("LoadType") == "SCD2").select("LoadType").distinct()
        print("LoadType")
        for path in paths:
            print("Entering into paths")
            sourceDF = spark.read.parquet(path)
            Source_table_name = path.split("/")[-4]
            display(Source_table_name)
            if Source_table_name == src_table_name  and LoadType.select("LoadType").collect()[0][0] == 'SCD2':
                # print("hey")
                stg_df = sourceDF.select(*[col(col_name) for col_name in tgt_cols]).distinct()
                for col_name in tgt_cols:
                   stg_df = stg_df.filter(col(col_name) != 'unknown')
                display(stg_df)
                joinDf = stg_df.alias('x').join(dim_df.alias('y'), expr(logic_col_list) & (dim_df["ActiveIndicator"] == 'Y'),
                                                 "left_outer") \
                    .select(
                    col(f"x.{src_cols_row[0].SourceColumnNames}").alias(f"Source_{tgt_cols_row[0].TargetColumnNames}"),
                    col(f"x.{src_cols_row[1].SourceColumnNames}").alias(f"Source_{tgt_cols_row[1].TargetColumnNames}"),
                    col(f"y.{tgt_cols_row[0].TargetColumnNames}"),
                    col(f"y.{tgt_cols_row[1].TargetColumnNames}"),
                    lit("Y").alias("ActiveIndicator"),
                    lit("2024-10-21").cast("timestamp").alias('startDate'),
                    lit(None).cast("timestamp").alias('endDate')
                )
                display(joinDf)  # Corrected line
                # Filter
                filterDf = joinDf.filter(xxhash64(joinDf[alias_columns_row[1]['AliasColumnNames']]) !=
                                          xxhash64(joinDf[tgt_cols_row[1].TargetColumnNames]))
                print("FilterDF")
                display(filterDf)
                # Merge
                mergeDf = filterDf.withColumn("MERGEKEY", filterDf[f"Source_{tgt_cols_row[0].TargetColumnNames}"])
                print("merge")
                display(mergeDf)
                # Dummy
                dummyDf = filterDf.filter(f"{tgt_cols_row[0].TargetColumnNames} is not null").withColumn("MERGEKEY", lit(None))
                print("Dummy")
                # Show dummyDf
                dummyDf.show()
                # Show scdf
                print("SCDF")
                scdDf = mergeDf.union(dummyDf)
                scdDf=scdDf
                scdDf.show()
                combined_records = targetTable.alias("target").merge(
                    source=scdDf.alias("source"),
                    condition=f"target.{tgt_cols_row[0].TargetColumnNames} == source.MERGEKEY and target.ActiveIndicator == 'Y'"
                ).whenMatchedUpdate(
                    set={
                        "ActiveIndicator": "'N'",
                        "endDate": F.current_date()
                    }
                ).whenNotMatchedInsert(
                    values={
                        tgt_cols_row[0].TargetColumnNames: F.col(f"source.{alias_columns_row[0]['AliasColumnNames']}"),
                        tgt_cols_row[1].TargetColumnNames: F.col(f"source.{alias_columns_row[1]['AliasColumnNames']}"),
                        "ActiveIndicator": F.lit('Y'),
                        "startDate": F.current_date(),
                        "endDate": F.lit(None) 
                    }
                ).execute()
                print("done")
            
    except Exception as e:
            print("An error occurred:", str(e))

for tables in DeltaTables:
    scdtype2(Paths, control_df, tables)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.dimmanager
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.dimemployee
# MAGIC

# COMMAND ----------


