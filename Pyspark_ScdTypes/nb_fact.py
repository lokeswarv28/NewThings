# Databricks notebook source
from datetime import datetime


today_date = datetime.now()
year = today_date.year
month = today_date.month
day = today_date.day
sql=f'''
create table dev.Fact_EmployeeActivity(
  Sno LONG GENERATED ALWAYS AS IDENTITY ,   
  FK_EmployeeKey long,
  FK_Designationkey long,
  AttendanceDate string,
  FirstInTime timestamp,
  LastOutTime timestamp,
  entryDate timestamp
)
LOCATION  "dbfs:/Project/Gold/dev.Fact_EmployeeActivity/{year}/{month}/{day}"
'''
spark.sql(sql)

# COMMAND ----------

dim_emp=spark.sql("select * from dev.dimemployee")
dim_manager=spark.sql("select * from dev.dimmanager")
dim_designation=spark.sql("select * from dev.dimdesignation")
dim_leavetype=spark.sql("select * from dev.dimleavetype")
fact_df1=spark.sql("select * from dev.Fact_EmployeeActivity ")
display(fact_df1)
display(dim_emp)

# COMMAND ----------

from pyspark.sql.functions import *
from delta import * 
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from datetime import *
today_date = datetime.now()
year = today_date.year
month = today_date.month
day = today_date.day

# Generate output paths
Paths = f"dbfs:/Project/Silver/EmployeeLoginDetails/{year}/{month}/{day}/"
# display(Paths)
# Assuming joinDf and df_dimemployee are defined
# Filter condition
source1 = spark.read.parquet(Paths)


display(source1)

source=source1.select('pk_empid', 'employeename','attendencedate' ,'firstintime',  'lastouttime', 'designation')
joindf1 = (source
          .select('pk_empid', 'employeename','attendencedate' ,'firstintime',  'lastouttime', 'designation')
          .join(dim_emp.select("pk_empid", "Pk_EmployeeTypeKey", col("endDate").alias("empEndDate")),
                source1["pk_empid"] == dim_emp["pk_empid"],
                "left")
          .filter(col("empEndDate").isNull())
          .join(dim_designation.select("Pk_DesignationKey", "designation"),
                source1["designation"] == dim_designation["designation"],
                "left")
         )

display(joindf1)
# joindf.count()


# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, current_date
fact_data1 = joindf1.select(
    col("Pk_EmployeeTypeKey").alias("FK_EmployeeKey"),
    col("Pk_DesignationKey").alias("FK_Designationkey"),
    col("attendencedate").alias("AttendanceDate"),
    to_timestamp(col("firstintime")).alias("FirstInTime"),
    to_timestamp(col("lastouttime")).alias("LastOutTime"),
    current_date().alias("entryDate")
)


# COMMAND ----------

fact_data1.count()

# COMMAND ----------

new_df = fact_data1.join(fact_df1, 'FK_EmployeeKey', 'left_anti').withColumn('entryDate', to_timestamp('entryDate'))

print("count_df", new_df.count())
print("done")
new_df.write.mode('append').option("mergeSchema", "true").saveAsTable('dev.Fact_EmployeeActivity')

# COMMAND ----------

s=spark.sql("select * from dev.Fact_EmployeeActivity")
display(s)

# COMMAND ----------

########################FACT2####################################################

# COMMAND ----------

from datetime import datetime


today_date = datetime.now()
year = today_date.year
month = today_date.month
day = today_date.day
sql=f'''
create table dev.Fact_EmployeeLeaves
(
    Sno BIGINT GENERATED ALWAYS AS IDENTITY,
    FK_EmployeeKey bigint,
    ManagerId string,
    FK_LeaveTypeKey bigint,
    Date  String,
    Days double,
    Reason string,
    entryDate timestamp
)
LOCATION "dbfs:/Project/Gold/dev.Fact_EmployeeLeaves/{year}/{month}/{day}"'''
spark.sql(sql)

# COMMAND ----------

dim_emp=spark.sql("select * from dev.dimemployee")
dim_manager=spark.sql("select * from dev.dimmanager")
dim_designation=spark.sql("select * from dev.dimdesignation")
dim_leavetype=spark.sql("select * from dev.dimleavetype")
fact_df=spark.sql("select * from dev.Fact_EmployeeLeaves ")


# COMMAND ----------

stg=spark.read.parquet(f"dbfs:/Project/Silver/Xyenta_Leaves_2022/{year}/{month}/{day}/")
stg = stg.filter((col("managerno") != "unknown") & (col("managername") != "unknown"))

display(stg)
joindf = (stg
          .select('employeeno', 'name','managerno', 'managername',  'leavetype', 'days','date','reason')
          .join(dim_emp.select("pk_empid", "Pk_EmployeeTypeKey", col("endDate").alias("empEndDate")),
                stg["employeeno"] == dim_emp["pk_empid"],
                "left")
          .filter(col("empEndDate").isNull())
          .join(dim_manager.select("managerno", "Pk_ManagerKey", col("endDate").alias("mgrEndDate")),
                stg["managerno"] == dim_manager["managerNo"],
                "left")
          .filter(col("mgrEndDate").isNull()) 
          .join(dim_leavetype.select("pk_LeaveTypeKey", "leavetype"),
                stg["leavetype"] == dim_leavetype["leavetype"],
                "left")
         )

display(joindf)

# COMMAND ----------

fact_data = joindf.select(
    col("Pk_EmployeeTypeKey").alias("FK_EmployeeKey"),
    col("dimmanager.managerno").alias("ManagerId"),  # Specify the table name
    col("pk_LeaveTypeKey").alias("FK_LeaveTypeKey"),
    col("date").alias("Date"),
    col("days").alias("Days"),
    col('reason').alias("Reason"),
    current_date().alias("entryDate")
)



# COMMAND ----------

display(fact_data)

# COMMAND ----------

new_df = fact_data.join(fact_df, 'ManagerId', 'left_anti').withColumn('entryDate', to_timestamp('entryDate'))

print("count_df", new_df.count())
display(new_df)
# new_df.count()
new_df.write.mode('append').option("mergeSchema", "true").saveAsTable('dev.Fact_EmployeeLeaves')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.Fact_EmployeeLeaves

# COMMAND ----------


