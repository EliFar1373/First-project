# Databricks notebook source

df=spark.sql('select * from databrickslearning2.default.mall_customers')
df.show()
display(df)

# COMMAND ----------

#Filtering data
filtered_df=df.filter(df['Annual_Income_(k$)']>50)
display(filtered_df)


# COMMAND ----------

# Select specific columns
selected_df=df.select('CustomerID','Annual_Income_(k$)')
display(selected_df)

# COMMAND ----------

# Add a new column with a constant value
addcolumn_df=df.withColumn('New_annual_income',df['Annual_Income_(k$)']+20)
display(addcolumn_df)


# COMMAND ----------

_

# COMMAND ----------

# Rename a column
renamed_df=df.withColumnRenamed('Annual_Income_(k$)','Annual_Income')
display(renamed_df)

# COMMAND ----------

# Drop a column
df=df.drop('Age')
display(df)

# COMMAND ----------

# Remove duplicates
df=df.dropDuplicates()


# COMMAND ----------

# Sort by annual income in descending order
display(df)

# COMMAND ----------

# Sort by annual income in descending order
df_sortted=df.orderBy(df['Annual_Income_(k$)'].desc())
display(df_sortted)

# COMMAND ----------

# Drop rows with any null values
dropped_na=df.dropna()

# Fill null values with a specific value
filled_na=df.fillna({'Annual_Income_(k$)':0,'Spending_Score':1})
display(filled_na)

# COMMAND ----------

# Calculate the average spending score by genre
avg_spending_score=df.groupBy('Genre').agg({'Spending_Score':'avg','Spending_Score':'sum'})
display(avg_spending_score)

# COMMAND ----------

from pyspark.sql.functions import avg,sum
# Calculate the average and sum of spending score by genre
avg_spending_score=df.groupBy('Genre').agg(avg('Spending_Score').alias('average_spending_score'),sum('Spending_Score').alias('sum_spending_score'))
display(avg_spending_score)

# COMMAND ----------

from pyspark.sql.functions import col
selected_df2=df.select(
    df['CustomerID'],
    df['Genre']

)

display(selected_df2)


# COMMAND ----------

from pyspark.sql.functions import col
selected_df3=df.select(
    col('CustomerID'),
    col('Genre')
)
display(selected_df3)

# COMMAND ----------

from pyspark.sql.functions import col
df_filtered_customer=df.filter((col('CustomerID')==160)|(col('CustomerID')==132))
display(df_filtered_customer)

# COMMAND ----------

#save a DataFrame as a table in the Databricks Unity Catalog
# Save a DataFrame as a table in the Databricks Unity Catalog
from pyspark.sql.functions import col
df_filtered_customer=df.filter((col('CustomerID')==160)|(col('CustomerID')==132))
df_filtered=df_filtered_customer.withColumnRenamed('Annual_Income_(k$)','Annual_Income')
df_filtered.write.mode('overwrite').saveAsTable('databrickslearning2.default.new_table_name')


# COMMAND ----------

# Set the Storage Account Key, "Access keys, spark.conf.set( "fs.azure.account.key.<storage-account-name>.blob.core.windows.net", "<your-storage-account-key>" )
spark.conf.set("fs.azure.account.key.learn2storage.blob.core.windows.net",
  "4kz4RcIOT3gmKbk5Kdo3KxlEkFK5z2T76eA1vXrdzUc4tWym4vGBOVRuMfu/QKItP1GPBr4/Rj6m+ASthgO6kA==")
# Set  file_path = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/<path-to-your-data>"
file_path="wasbs://first@learn2storage.blob.core.windows.net/first/output.csv"
df_filtered.write.format('csv').mode('overwrite').save(file_path)



# COMMAND ----------

df=df.withColumnRenamed('Annual_Income_(k$)','Annual_Income')

# Set the storage account key
spark.conf.set(
    "fs.azure.account.key.learn2storage.dfs.core.windows.net",
    "4kz4RcIOT3gmKbk5Kdo3KxlEkFK5z2T76eA1vXrdzUc4tWym4vGBOVRuMfu/QKItP1GPBr4/Rj6m+ASthgO6kA=="
)

# Set the file path
file_path = "abfss://first@learn2storage.dfs.core.windows.net/first/new.delta"

# Save the DataFrame as a Delta table
df.write.format("delta").mode("overwrite").save(file_path)

# COMMAND ----------


