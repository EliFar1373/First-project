# Databricks notebook source
# List files in the specified directory
files=dbutils.fs.ls("/FileStore/tables/")
display(files)

# COMMAND ----------



# COMMAND ----------

# File location and type
df=spark.read.format("csv").option("header","true").option("sep", ",").load("dbfs:/FileStore/tables/sales_data_sample.csv")
display(df)
df.show()

# COMMAND ----------

# Create a temporary view
temp_table_name="sales_data"
df.createOrReplaceTempView(temp_table_name)


# Create a permanat view
permanent_table_name="sales_data_per"
df.createOrReplaceGlobalTempView(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Then, you can query it using SQL:
# MAGIC SELECT * FROM sales_data;

# COMMAND ----------


#Create a Permanent View using sql
#First, create a table from the DataFrame:
df.write.format("delta").mode("overwrite").saveAsTable("my_table2")




spark.sql("create temporary view my_view3 as select * from my_table2" )
spark.sql("create view my_view4 as select * from my_table2")




# COMMAND ----------

# MAGIC %sql
# MAGIC --Create Create a Common Table Expression (CTE)
# MAGIC with sales_data_cte as(
# MAGIC   select * from my_view
# MAGIC   where "SALES">200
# MAGIC )
# MAGIC select * from sales_data_cte ;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count_if(PRICEEACH > 90) FROM sales_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Count distinct
# MAGIC select count_if(DISTINCT PRICEEACH > 90 ) from sales_data ;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC --Count Null Values:
# MAGIC select count(*) from sales_data where PRICEEACH IS NULL;

# COMMAND ----------

# Set Up JDBC Connection Properties

jdbc_url = "jdbc:mysql://localhost:3306/word"
table_name = "city"
properties = {
    "user": "root",
    "password": "Elham$$123"
}

#Read Data from JDBC
 #Read Data from JDBC
jdbc_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
display(jdbc_df)
#Save as a Delta Table
jdbc_df.write.format("delta").saveAsTable("jdbc_table")

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second

# Create a DataFrame
data = [("2024-11-07 12:34:56", 123)]
columns = ["date_string", "some_value"]
df = spark.createDataFrame(data, columns)

# Cast the date_string column to a TIMESTAMP
df = df.withColumn("timestamp_value", df["date_string"].cast("timestamp"))

# Extract calendar data from the timestamp
df = df.withColumn("year", year(df["timestamp_value"])) \
       .withColumn("month", month(df["timestamp_value"])) \
       .withColumn("day", dayofmonth(df["timestamp_value"])) \
       .withColumn("hour", hour(df["timestamp_value"])) \
       .withColumn("minute", minute(df["timestamp_value"])) \
       .withColumn("second", second(df["timestamp_value"]))

# Display the DataFrame
display(df)

# COMMAND ----------


# Create a DataFrame
data = [("2024-11-07 12:34:56", 123)]
columns = ["date_string", "some_value"]
df = spark.createDataFrame(data, columns)


df.createOrReplaceGlobalTempView("global_temp_view_name")

result=spark.sql("""
          select
           date_string, cast(date_string as TIMESTAMP) AS timestamp_value
          from global_temp.global_temp_view_name

 """)

display(result)

# COMMAND ----------

# Create a DataFrame
data = [("2024-11-07 12:34:56", 123)]
columns = ["date_string", "some_value"]
df = spark.createDataFrame(data, columns)


df.createOrReplaceGlobalTempView("global_temp_view_name")
result=spark.sql("""
                 SELECT
                 YEAR(TIMESTAMP_value) as year,
                 MONTH(TIMESTAMP_value) as month,
                 DAY(timestamp_value) AS day,
                 HOUR(timestamp_value) AS hour,
                 MINUTE(timestamp_value) AS minute,
                 SECOND(timestamp_value) AS second
                  from(
                      SELECT
                      date_string,cast(date_string as TIMESTAMP )as TIMESTAMP_value
                       from global_temp.global_temp_view_name )
                         as subquery;

""")

display(result)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databrickslearning2.default.mall_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC case 
# MAGIC when Age>30 then 0
# MAGIC when Age<30 then 1
# MAGIC else  Null
# MAGIC End as alias_name
# MAGIC from databrickslearning2.default.mall_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC  Incremental Data Processing

# COMMAND ----------

# MAGIC %sql
# MAGIC create table managed_table(
# MAGIC   id int,
# MAGIC   name string
# MAGIC )
# MAGIC using delta;
# MAGIC
# MAGIC create table external_table(
# MAGIC   id int,
# MAGIC   name string
# MAGIC )
# MAGIC using delta
# MAGIC location '';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE detail managed_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY managed_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into managed_table values(1,'eli');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history managed_table;

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC --Roll back a table to a previous version
# MAGIC restore table managed_table to version as of 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from managed_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Review a history of table transactions
# MAGIC describe history managed_table;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC --Query a specific version of a table
# MAGIC select * from managed_table version as of 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Vacuum in Delta Lake
# MAGIC VACUUM managed_table retain 168 hours;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Optimize Command in Delta Lake
# MAGIC OPTIMIZE managed_table;

# COMMAND ----------


