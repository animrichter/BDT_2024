-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### TABLE JOINS
-- MAGIC
-- MAGIC https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-qry-select-join.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### EXERCISE
-- MAGIC
-- MAGIC - join tables created in the previous notebook (**employee** and **employee_salary**) and display results
-- MAGIC - Lower the salary of 'Mia Lewiski' by 10% - use **SQL**
-- MAGIC - Rise salary by 50% to 'Sara Parker' - use **PySpark** (hint: from pyspark.sql.functions import col, when)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC delta time travel: 
-- MAGIC - https://docs.databricks.com/en/delta/history.html
-- MAGIC - https://www.databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - use **DESCRIBE HISTORY** command to table **employee_salary** and explore results

-- COMMAND ----------

DESCRIBE HISTORY employee_salary

-- COMMAND ----------

-- How to SELECT historic data in a delta table:

SELECT * FROM employee_salary VERSION AS OF 1;
SELECT * FROM employee_salary@v1;
SELECT * FROM employee_salary TIMESTAMP AS OF "2023-10-09 13:20:48.0"

-- COMMAND ----------

-- How to RESTORE historic data in a delta table:

RESTORE TABLE employee_salary TO VERSION AS OF 2

-- COMMAND ----------

SELECT * FROM employee_salary

-- COMMAND ----------

DESCRIBE HISTORY employee_salary;
DESCRIBE DETAIL employee_salary;
