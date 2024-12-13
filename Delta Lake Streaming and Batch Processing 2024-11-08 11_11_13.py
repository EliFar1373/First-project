# Databricks notebook source
# MAGIC %md
# MAGIC Creating catalog tables

# COMMAND ----------

# Create a DataFrame with specified values
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Display the DataFrame
display(df)

# COMMAND ----------

# Save a dataframe as a managed table
## specify a path option to save as an external table
##df.write.format("delta").option("path", "/mydata").saveAsTable("MyExternalTable")
df.write.format('delta').saveAsTable('newtable')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS SMyExternalTable
# MAGIC (
# MAGIC     Orderid INT NOT NULL,
# MAGIC     OrderDate TIMESTAMP NOT NULL,
# MAGIC     CustomerName STRING,
# MAGIC     SalesTotal FLOAT NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'path/to/location'
# MAGIC
# MAGIC
# MAGIC  

# COMMAND ----------

from delta.tables import *
DeltaTable.createIfNotExists(spark)\
  .tableName('thirdtable').addColumn('productId','INT').addColumn('productname','string').execute()


# COMMAND ----------

# MAGIC %md
# MAGIC Creating a Delta Lake table from a dataframe

# COMMAND ----------

# Create a DataFrame with specified values
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

#  Set the storage account key
spark.conf.set(
    "fs.azure.account.key.learn2storage.dfs.core.windows.net",
    "4kz4RcIOT3gmKbk5Kdo3KxlEkFK5z2T76eA1vXrdzUc4tWym4vGBOVRuMfu/QKItP1GPBr4/Rj6m+ASthgO6kA=="
)

# Set the file path
file_path = "abfss://first@learn2storage.dfs.core.windows.net/first/second.delta"

# Save the DataFrame as a Delta table 
# new_rows_df.write.format("delta").mode("append").save(delta_table_path)
df.write.format("delta").mode("overwrite").save(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Streaming 

# COMMAND ----------

# Create a DataFrame with sample data
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Save the DataFrame as a Delta table
df.write.format("delta").mode("overwrite").save("/tmp/delta/people")

# COMMAND ----------

# MAGIC %md
# MAGIC Create a Delta Table

# COMMAND ----------

# Create a DataFrame with sample data
data=[('Alice',34),('Eli',20)]
columns=['Name','Age']
df=spark.createDataFrame(data,columns)

# Save the DataFrame as a Delta table
df.write.format('delta').mode('overwrite').save('/tmp/delta/customer')


# COMMAND ----------

#Read the Delta Table
df_read=spark.read.format('delta').load('/tmp/delta/customer')
display(df_read)

# COMMAND ----------

#Set Up Streaming Query
# Read the Delta table as a streaming source
streaming_df=spark.readStream.format('delta').load('/tmp/delta/customer')
display(streaming_df)



# COMMAND ----------

#Write Streaming Data to a Delta Table
output_path='/tmp/delta/customer_streaming_output'
checkpoint_path = '/tmp/delta/people_streaming_checkpoint'
# Write the streaming DataFrame to a Delta table
query=streaming_df.writeStream.format('delta').option("checkpointLocation", checkpoint_path).outputMode('append').start(output_path)


# COMMAND ----------

# Function to append new data to the Delta table
def append_data():
    new_data=[('joe',31),('jay',34)]
    columns=['Name','Age']
    new_df=spark.createDataFrame(data,columns)
    new_df.write.format('delta').mode('append').save('tmp/delta/customer')
import time 
# Append new data every 10 seconds
for i in range(5):
   append_data()
   time.sleep(10) 

# COMMAND ----------

# Stop the Streaming Query
query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC Querying Delta Tables with Spark SQL

# COMMAND ----------

spark.read.format('delta').load('/tmp/delta/customer').createOrReplaceTempView('customer_info')
result_df=spark.sql('select * from customer_info')
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Querying catalog tables
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mall_customers

# COMMAND ----------

df2=spark.read.format('delta').load('/tmp/delta/customer')
df2.write.format('csv').mode('overwrite').save('/tmp/delta/customer_csv')
df2.write.format('parquet').mode('overwrite').save('/tmp/delta/customer_parquet')






# COMMAND ----------

# MAGIC %md
# MAGIC Partition the output file

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mall_customers

# COMMAND ----------

# Transfer the SQL query result to a DataFrame
df3=spark.sql('select * from databrickslearning2.default.mall_customers')
df3.write.mode('overwrite').format('csv').partitionBy('Genre').save('/tmp/csv/')


# COMMAND ----------

# MAGIC %fs ls /tmp/csv

# COMMAND ----------

# Create a managed table in the catalog from the DataFrame 'df3'
df3 = df3.withColumnRenamed("Annual_Income_(k$)", "Annual_Income_k")
df3.write.mode('overwrite').saveAsTable('databrickslearning2.default.managed_mall_customers2')


