# Databricks notebook source
# MAGIC %md
# MAGIC >>Validating the dataframe from Bronze To Silver
# MAGIC
# MAGIC >>.Column Comparison 
# MAGIC
# MAGIC >>.Schema Comparison
# MAGIC
# MAGIC >>.Dates Comaparison
# MAGIC
# MAGIC >>.Datatypes Comparison
# MAGIC

# COMMAND ----------

df=spark.read.csv('dbfs:/FileStore/Xyenta_Leaves_2022_2.csv',header=True)
display(df)

# COMMAND ----------

# %run /HR_ANALYTICS/NB_LOGS

# COMMAND ----------

# MAGIC %run /HR_ANALYTICS/NB_SETUP

# COMMAND ----------

display(filepaths)

# COMMAND ----------

display(df_metadata_columns)

# COMMAND ----------


# if 'column_logs3' not in locals():

#     column_logs3=LogsConfig2()





# COMMAND ----------

# MAGIC %md
# MAGIC LOGS 

# COMMAND ----------

# def column_comparisons():
#     try:
#         column_logs3.logger.info('test')
#     except Exception as e:
#         raise e



# COMMAND ----------

# column_comparisons()

# COMMAND ----------

# MAGIC %md
# MAGIC COLUMN COMPARISON

# COMMAND ----------

from pyspark.sql.functions import col

def column_comparison(filepaths, df_metadata_columns):
    try:
        for x in filepaths.collect():
            path = x['FilePath']
            stg = x['StagingNames']
            FullPath = f"{path}/{stg}/{year}/{month}/{day}/"
            dfs = spark.read.csv(FullPath, header=True, inferSchema=True)
            dfs_columns = set(map(str.lower, dfs.columns))
            dfs_columns = {column.replace(" ", "") for column in dfs_columns}
            ref_filter = df_metadata_columns.filter(col('StagingNames') == stg)
            ref_columns = set(map(str.lower, ref_filter.select('Columns').rdd.flatMap(lambda x: x).collect()))
            missing_columns = dfs_columns - ref_columns 
            if missing_columns:
                error_message = "Column names don't match for {} and missing columns are: {}".format(stg, missing_columns)
                raise ValueError(error_message)
            else:
                print("All columns are present for {}.".format(stg))
                # Construct the path for saving Parquet file
                parquet_path = f"/Project/Silver/{stg}/{year}/{month}/{day}/"
                dfs.write.format('parquet').mode('overwrite').save(parquet_path)

    except Exception as e:
        raise e




# COMMAND ----------

column_comparison(filepaths,df_metadata_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC Schema Comparison

# COMMAND ----------

def SchemaComparison(filepaths, df_metadata_columns):
    try:
        for x in filepaths.collect():
            path = x['FilePath']
            stg = x['StagingNames']
            FullPath = f"{path}/{stg}/{year}/{month}/{day}/"
            dfs = spark.read.csv(FullPath, header=True, inferSchema=True)
            dfs2 = dfs.toDF(*[col.lower().replace(" ", "") for col in dfs.columns])
            # dfs.printSchema()
            print("dfs2....")
            dfs2.printSchema()
            # display(dfs)
            ref_filter = df_metadata_columns.filter(col('StagingNames') == stg)
            # display(ref_filter)
            for x in ref_filter.collect():
                columnnames = x['Columns']
                refTypes = x['TargetDataType']

                columnnamesList = [x.strip().lower() for x in columnnames.split(",")]
                refTypeList = [x.strip().lower() for x in refTypes.split(",")]
                # print(columnnamesList)
                # print(refTypeList)
                dfsTypes=dfs2.schema[columnnames.lower()].dataType.simpleString()
                print(dfsTypes)
                dfsTypesList=[x.strip().lower() for x in dfsTypes.split(",")]
                # print("dfs2types list")
                
                missmatchedcolumns = [(col_name, df_type, ref_type) for (col_name, df_type, ref_type) in
                                     zip(columnnamesList, dfsTypesList, refTypeList) if dfsTypesList!=  refTypeList]
                if missmatchedcolumns:
                    print("schema comparison has been failed or mismatched for this {}".format(path))
                    for col_name,df_type,ref_type in missmatchedcolumns:
                        print(f"columname:{col_name},DataFrametype:{df_type},referenceType:{ref_type}")
                else:
                    print("Schema comparison is done and scucess for {}".format(path))
                    parquet_path = f"/Project/Silver/{stg}/{year}/{month}/{day}/"
                    dfs.write.format('parquet').mode('overwrite').save(parquet_path)


                
    except Exception as e:
        raise e


# COMMAND ----------

SchemaComparison(filepaths,df_metadata_columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Null Counts

# COMMAND ----------

def CheckForNullF(filepaths, df_metadata_columns):
    try:
        # logs_null.logger.info("Executing CheckForNull method...& get the filename and path from controldf")
        for x in filepaths.collect():
            path = x['FilePath']
            stg = x['StagingNames']
            FullPath = f"/Project/Silver/{stg}/{year}/{month}/{day}/"
            dfs = spark.read.parquet(FullPath, header=True, inferSchema=True)
            dfs = dfs.toDF(*[col.lower().replace(" ", "") for col in dfs.columns])
            all_non_null_dfs = []
            # logs_null.logger.warning("Executing the actual logic for null count...")
            print("Executing the actual logic for null count...")
 
            # Filter rows with at least one null value 
            null_rows = dfs.filter(greatest(*[col(x).isNull() for x in dfs.columns]))  
            # null_rows.show(30)
            # Get the count of rows with at least one null value
            null_rows_count = null_rows.count()
            print(f'Null rows count::{null_rows_count}')
 
            # Filter rows with all non-null values
            non_null_rows = dfs.filter(least(*[col(x).isNotNull() for x in dfs.columns]))
            print(f'non null rows ::{non_null_rows.count()}')
            # display(non_null_rows)
        
 
            # Write non-null rows to the "good" path
            if non_null_rows.count() > 0:
                print("non null rows count::: condition")
                output_good_path =f"/Project/Silver/{stg}/{year}/{month}/{day}/"
                df_nulls=non_null_rows.coalesce(1).write.mode('overwrite').option("header", True).parquet(output_good_path)
                print('no_null_rows')
                # display(df_nulls)
                # logs_null.logger.info("Successfully written file {} to {}".format(filename, output_good_path))
                print("Successfully written file {} to {}".format(stg, output_good_path))
                all_non_null_dfs.append(non_null_rows)
            
 
            # Write null rows to the "bad" path
            if null_rows_count > 0:
                output_bad_path = f'dbfs:/mnt/BadRecords/{stg}-NullRecords/'
                null_rows.coalesce(1).write.mode('overwrite').option("header", True).parquet(output_bad_path)
                # logs_null.logger.info("Writing records with null values to bad record path")  
                print("Writing records with null values to bad record path")      
            else:
                # logs_null.logger.info("No records without null values or empty spaces found for {}".format(filename))
                print("No records without null values or empty spaces found for {}".format(stg))
        return  all_non_null_dfs    
    except Exception as e:
        # logs_null.logger.error("An error occurred while executing CheckForNull::", str(e))
        raise e


# COMMAND ----------

CheckForNullF(filepaths, df_metadata_columns)

# COMMAND ----------

# all_non_null_dfs = CheckForNullF(filepaths, df_metadata_columns)

# # Iterate through the list of non-null dataframes
# # for df in all_non_null_dfs:
#   # Display each dataframe
#   display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC checking the null values in silver

# COMMAND ----------

#leavetype','date'
#'attendencedate'


# COMMAND ----------

# MAGIC %md
# MAGIC ##DUPLICATE RECORDS

# COMMAND ----------

display(df_metadata_columns)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, desc, row_number

# COMMAND ----------

# display(df_metadata_columns)
df_duplicate=df_metadata_columns.select('StagingNames','FilePath','GroupByColumn').distinct()
display(df_duplicate)

# COMMAND ----------

def duplicate_records(filepaths, df):
    for x in df.collect():
        path = x['FilePath']
        stg = x['StagingNames']
        grpname = x['GroupByColumn']
        all_grpnames = []
        FullPath = f"/Project/Silver/{stg}/{year}/{month}/{day}/"
        dfs = spark.read.parquet(FullPath, header=True, inferSchema=True)
        dfs = dfs.toDF(*[col.lower().replace(" ", "") for col in dfs.columns])
        
        if stg == 'Xyenta_Leaves_2022':
            window_spec = Window().partitionBy('leavetype', 'employeeno', 'date').orderBy(desc('employeeno'))
            duplicate_df = dfs.withColumn('rn', row_number().over(window_spec))
            duplicate_records = duplicate_df.filter(col('rn') != 1)
            duplicate_count = duplicate_records.count()
            print(f"duplicate counts {stg}:{duplicate_count}")
            non_duplicate = duplicate_df.filter(col('rn') == 1)
            non_duplicate_count = non_duplicate.count()
            print(f"non_duplicate :{'Xyenta_Leaves_2022'}-->{non_duplicate_count}")
            display(non_duplicate)
            output_good_path = f"/Project/Silver/{stg}/{year}/{month}/{day}/"
            non_duplicate.coalesce(1).write.mode('overwrite').option("header", True).parquet(output_good_path)
            print("successfully updated in Silver {stg}")

        elif stg == 'EmployeeLoginDetails':
            window_spec = Window().partitionBy('pk_empid', 'attendencedate').orderBy(desc('pk_empid'))
            duplicate_df = dfs.withColumn('rn', row_number().over(window_spec))
            duplicate_records = duplicate_df.filter(col('rn') != 1)
            duplicate_count = duplicate_records.count()
            print(f"duplicate counts:{stg}--->{duplicate_count}")
            non_duplicate = duplicate_df.filter(col('rn') == 1)
            non_duplicate_count = non_duplicate.count()
            print(f"non_duplicate ---> {stg}--->{non_duplicate_count}")
            display(non_duplicate)
            output_good_path = f"/Project/Silver/{stg}/{year}/{month}/{day}/"
            non_duplicate.coalesce(1).write.mode('overwrite').option("header", True).parquet(output_good_path)
            print("successfully updated in Silver {stg}")
        else:
            window_spec = Window().partitionBy('holiday', 'date').orderBy(desc('holiday'))
            duplicate_df = dfs.withColumn('rn', row_number().over(window_spec))
            duplicate_records = duplicate_df.filter(col('rn') != 1)
            duplicate_count = duplicate_records.count()
            print(f"duplicate counts:{stg}--->{duplicate_count}")
            non_duplicate = duplicate_df.filter(col('rn') == 1)
            non_duplicate_count = non_duplicate.count()
            print(f"non_duplicate ---> {stg}--->{non_duplicate_count}")
            display(non_duplicate)
            output_good_path = f"/Project/Silver/{stg}/{year}/{month}/{day}/"
            non_duplicate.coalesce(1).write.mode('overwrite').option("header", True).parquet(output_good_path)
            print("successfully updated in Silver {stg}")


# COMMAND ----------

duplicate_records(filepaths, df_duplicate)

# COMMAND ----------


# data = [(10, 20, 30), (50, 40, 30), (1, 2, 3)]
# df = spark.createDataFrame(data, ["col1", "col2", "col3"])


# COMMAND ----------

# df_greatest=df.withColumn("greatest",greatest(*[col(x) for x in df.columns])).show()

# COMMAND ----------

df_max=df.select(max(df.col1), max(df.col2)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC sucess checkfornull

# COMMAND ----------

# def CheckForNullF(filepaths, df_metadata_columns):
#     try:
#         # Initialize an empty list to store paths for non-null data
#         non_null_paths = []
        
#         for x in filepaths.collect():
#             path = x['FilePath']
#             stg = x['StagingNames']
#             FullPath = f"{path}/{stg}/{year}/{month}/{day}/"
            
#             # Read the CSV file
#             dfs = spark.read.csv(FullPath, header=True, inferSchema=True)
#             dfs = dfs.toDF(*[col.lower().replace(" ", "") for col in dfs.columns])
            
#             print("Executing the actual logic for null count...")
 
#             # Filter rows with at least one null value 
#             null_rows = dfs.filter(greatest(*[col(x).isNull() for x in dfs.columns]))  
#             null_rows_count = null_rows.count()
#             print(f'Null rows count: {null_rows_count}')
 
#             # Filter rows with all non-null values
#             non_null_rows = dfs.filter(least(*[col(x).isNotNull() for x in dfs.columns]))
#             print(f'Non-null rows count: {non_null_rows.count()}')
        
#             # Write non-null rows to the "good" path
#             if non_null_rows.count() > 0:
#                 print("Writing non-null rows to the good path...")
#                 output_good_path = f"/Project/Silver/{stg}/{year}/{month}/{day}/"
#                 non_null_rows.coalesce(1).write.mode('overwrite').option("header", True).parquet(output_good_path)
#                 print(f"Successfully written file {stg} to {output_good_path}")
#                 non_null_paths.append(output_good_path)
            
#             # Write null rows to the "bad" path
#             if null_rows_count > 0:
#                 output_bad_path = f'dbfs:/mnt/raw/BadRecords/{stg}-NullRecords/'
#                 null_rows.coalesce(1).write.mode('overwrite').option("header", True).parquet(output_bad_path)
#                 print("Writing records with null values to bad record path")
#             else:
#                 print(f"No records with null values or empty spaces found for {stg}")
        
#         return non_null_paths
    
#     except Exception as e:
#         raise e

# # Call the function and get the list of paths for non-null data
# non_null_paths = CheckForNullF(filepaths, df_metadata_columns)

# # # Iterate through the list and read each DataFrame back from Parquet
# # all_non_null_dfs = []
# # for path in non_null_paths:
# #     non_null_df = spark.read.parquet(path)
# #     all_non_null_dfs.append(non_null_df)
# #     display(non_null_df)


# COMMAND ----------

# display(df_metadata_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Date Validation For Xyenta_Leaves_2022
# MAGIC

# COMMAND ----------

# filenames = ['Xyenta_Leaves_2022', 'EmployeeLoginDetails']

def datevalidation(filepaths, df_metadata_columns):
    try:
        for x in filepaths.collect():
            path = x['FilePath']
            stg = x['StagingNames']
            FullPath = f"{path}/{stg}/{year}/{month}/{day}/"
            dfs = spark.read.csv(FullPath, header=True, inferSchema=True).withColumn('inputfilename',  input_file_name()).withColumn('filename', split(col('inputfilename'), '/').getItem(3))
            
            ref_filter = df_metadata_columns.filter(col('StagingNames') == stg)
        
            IsDatecompare = ref_filter.filter(col('IsDate')=='Y')
            
            if dfs.filter(col('filename') == 'Xyenta_Leaves_2022').count() > 0:
                for row in IsDatecompare.collect():
                    date_column = row['Columns']
                    format_check = 'dd MMM yyyy'
                    print("hello")
                    dfs = dfs.withColumn(
                        'bad_record',
                        when(col(date_column).isNull(), "False")
                        .when(to_date(col(date_column), format_check).isNotNull(), "True")
                        .otherwise("False")
                    )
            
                    bad_records_df = dfs.filter(col('bad_record') == "False")
                    good_records_df = dfs.filter(col('bad_record') == "True")

                    bad_record_df_count = bad_records_df.count()
                    print("bad_records_count")
                    print(bad_record_df_count)
                    display(bad_records_df)
                    good_records_df_count = good_records_df.count()
                    print("good_records")
                    display(good_records_df)

                if bad_record_df_count > 0 :
                    print("entered into bad_records...")
                    bad_path = f"dbfs:/Project/BadRecords/{stg}-DateMissmatched/"
                    # bad_records_df.coalesce(1).write.mode('overwrite').option('Header', True).format('parquet').save(bad_path)
                    print("written to bad path")
                
                if good_records_df_count > 0:
                    print("entered into good_records...")
                    good_path = f"dbfs:/Project/Silver/{stg}/{year}/{month}/{day}"
                    good_records_df.coalesce(1).write.mode('overwrite').option('Header', True).format('parquet').save(good_path)
                    print("done written to silver path")

                else : 
                    print("No date column found..")

    except Exception as e:
        print(f"An error occurred while processing filename '{stg}': {str(e)}")


# COMMAND ----------

datevalidation(filepaths, df_metadata_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ##DATE VALIDATION FOR EMPLOYEELOGINDETAILS
# MAGIC

# COMMAND ----------

# filenames = ['Xyenta_Leaves_2022', 'EmployeeLoginDetails']

def datevalidation(filepaths, df_metadata_columns):
    try:
        for x in filepaths.collect():
            path = x['FilePath']
            stg = x['StagingNames']
            FullPath = f"{path}/{stg}/{year}/{month}/{day}/"
            
            # Read CSV file
            dfs = spark.read.csv(FullPath, header=True, inferSchema=True)\
                        .withColumn('inputfilename',  input_file_name())\
                        .withColumn('filename', split(col('inputfilename'), '/').getItem(3))
            
            # Filter metadata columns
            ref_filter = df_metadata_columns.filter(col('StagingNames') == stg)
            IsDatecompare = ref_filter.filter(col('IsDate') == 'Y')
            
            # Check if filename matches 'EmployeeLoginDetails'
            if dfs.filter(col('filename') == 'EmployeeLoginDetails').count() > 0:
                for row in IsDatecompare.collect():
                    date_column = row['Columns']
                    format_check = "dd-MM-yyyy HH:mm"
                    print("hello")
                    
                    # Validate date column
                    dfs = dfs.withColumn(
                        'bad_record',
                        when(col(date_column).isNull(), "False")
                        .when(to_date(col(date_column), format_check).isNotNull(), "True")
                        .otherwise("False")
                    )
                    
                    # Filter bad and good records
                    bad_records_df = dfs.filter(col('bad_record') == "False")
                    good_records_df = dfs.filter(col('bad_record') == "True")

                    bad_record_df_count = bad_records_df.count()
                    print("bad_records_count")
                    print(bad_record_df_count)
                    display(bad_records_df)
                    good_records_df_count = good_records_df.count()
                    print("good_records")
                    display(good_records_df)

                # Save bad records to path
                if bad_record_df_count > 0 :
                    print("entered into bad_records...")
                    bad_path = f"dbfs:/Project/BadRecords/{stg}-DateMissmatched/"
                    bad_records_df.coalesce(1).write.mode('overwrite').option('Header', True).format('parquet').save(bad_path)
                    print("written to bad path")
                
                # Save good records to path
                if good_records_df_count > 0:
                    print("entered into good_records...")
                    good_path = f"dbfs:/Project/Silver/{stg}/{year}/{month}/{day}"
                    good_records_df.coalesce(1).write.mode('overwrite').option('Header', True).format('parquet').save(good_path)
                    print("done written to silver path")

                else: 
                    print("No date column found..")

    except Exception as e:
        print(f"An error occurred while processing filename '{stg}': {str(e)}")


# COMMAND ----------

datevalidation(filepaths, df_metadata_columns)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Date Validation For Holidays
# MAGIC

# COMMAND ----------

# filenames = ['Xyenta_Leaves_2022', 'EmployeeLoginDetails']

def datevalidation_holidays(filepaths, df_metadata_columns):
    try:
        for x in filepaths.collect():
            path = x['FilePath']
            stg = x['StagingNames']
            FullPath = f"{path}/{stg}/{year}/{month}/{day}/"
            
            # Read CSV file
            dfs = spark.read.csv(FullPath, header=True, inferSchema=True)\
                        .withColumn('inputfilename',  input_file_name())\
                        .withColumn('filename', split(col('inputfilename'), '/').getItem(3))
            
            # Filter metadata columns
            ref_filter = df_metadata_columns.filter(col('StagingNames') == stg)
            IsDatecompare = ref_filter.filter(col('IsDate') == 'Y')
            
            # Check if filename matches 'EmployeeLoginDetails'
            if dfs.filter(col('filename') == 'XyentaHolidays').count() > 0:
                for row in IsDatecompare.collect():
                    date_column = row['Columns']
                    format_check = "dd-MM-yyyy HH:mm"
                    print("hello")
                    
                    # Validate date column
                    dfs = dfs.withColumn(
                        'bad_record',
                        when(col(date_column).isNull(), "False")
                        .when(to_date(col(date_column), format_check).isNotNull(), "True")
                        .otherwise("False")
                    )
                    
                    # Filter bad and good records
                    bad_records_df = dfs.filter(col('bad_record') == "False")
                    good_records_df = dfs.filter(col('bad_record') == "True")

                    bad_record_df_count = bad_records_df.count()
                    print("bad_records_count")
                    print(bad_record_df_count)
                    display(bad_records_df)
                    good_records_df_count = good_records_df.count()
                    print("good_records")
                    display(good_records_df)

                # Save bad records to path
                if bad_record_df_count > 0 :
                    print("entered into bad_records...")
                    bad_path = f"dbfs:/Project/BadRecords/{stg}-DateMissmatched/"
                    bad_records_df.coalesce(1).write.mode('overwrite').option('Header', True).format('parquet').save(bad_path)
                    print("written to bad path")
                
                # Save good records to path
                if good_records_df_count > 0:
                    print("entered into good_records...")
                    good_path = f"dbfs:/Project/Silver/{stg}/{year}/{month}/{day}"
                    good_records_df.coalesce(1).write.mode('overwrite').option('Header', True).format('parquet').save(good_path)
                    print("done written to silver path")

                else: 
                    print("No date column found..")

    except Exception as e:
        print(f"An error occurred while processing filename '{stg}': {str(e)}")


# COMMAND ----------

datevalidation_holidays(filepaths, df_metadata_columns)

# COMMAND ----------

# df=spark.read.csv('dbfs:/FileStore/Xyenta_Leaves_2022_2.csv',header=True)
# display(df)
# format_check = "dd-MMM-yy"
# print("hello")
# date_column='HDate'                   
# # Validate date column
# dfs = df.withColumn(
#                         'bad_record',
#                         when(col(date_column).isNull(), "False")
#                         .when(to_date(col(date_column), format_check).isNotNull(), "True")
#                         .otherwise("False")
#                     )
# bad_records_df = dfs.filter(col('bad_record') == "False")
# good_records_df = dfs.filter(col('bad_record') == "True")
# bad_record_df_count = bad_records_df.count()
# print("bad_records_count")
# print(bad_record_df_count)
# display(bad_records_df)
# good_records_df_count = good_records_df.count()
# print("good_records")
# display(good_records_df)

# COMMAND ----------


