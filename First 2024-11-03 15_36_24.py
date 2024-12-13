# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM databrickslearning2.default.sales_data_sample;

# COMMAND ----------

# MAGIC %sql
# MAGIC select ORDERLINENUMBER,CUSTOMERNAME,SALES from databrickslearning2.default.sales_data_sample;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM databrickslearning2.default.sales_data_sample where SALES>2000;

# COMMAND ----------

# MAGIC %sql
# MAGIC Delete from databrickslearning2.default.sales_data_sample where ORDERNUMBER=14;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update databrickslearning2.default.sales_data_sample 
# MAGIC set STATUS="Shipped" where ORDERNUMBER=10414;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databrickslearning2.default.sales_data_sample where ORDERNUMBER=10414;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from databrickslearning2.default.sales_data_sample;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databrickslearning2.default.sales_data_sample limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databrickslearning2.default.sales_data_sample order by SALES desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select YEAR_ID,SUM(SALES) as total_sales 
# MAGIC from databrickslearning2.default.sales_data_sample
# MAGIC group by YEAR_ID;

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(SALES) as total_sales , YEAR_ID
# MAGIC from databrickslearning2.default.sales_data_sample
# MAGIC
# MAGIC group by YEAR_ID
# MAGIC having total_sales>3000;

# COMMAND ----------

# MAGIC %sql
# MAGIC /* subquery
# MAGIC */
# MAGIC select * from databrickslearning2.default.sales_data_sample where SALES> (SELECT AVG(SALES) from databrickslearning2.default.sales_data_sample );

# COMMAND ----------

# MAGIC %sql
# MAGIC /* 
# MAGIC */
# MAGIC
# MAGIC SELECT CUSTOMERNAME, UPPER(CUSTOMERNAME) as uppername,
# MAGIC LENGTH(CUSTOMERNAME)
# MAGIC from databrickslearning2.default.sales_data_sample;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW sales_summary
# MAGIC as 
# MAGIC select YEAR_ID, SUM(SALES) as total_sales
# MAGIC from databrickslearning2.default.sales_data_sample 
# MAGIC group by YEAR_ID;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS sales_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view temp_sales as
# MAGIC select * from databrickslearning2.default.sales_data_sample
# MAGIC where SALES<2000; 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
