-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### VIEWS
-- MAGIC
-- MAGIC https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-view.html
-- MAGIC
-- MAGIC Two types of temporary views can be created:
-- MAGIC
-- MAGIC **Session scoped temporary view**:
-- MAGIC - is only available with a spark session, so another notebook in the same cluster can not access it. 
-- MAGIC - if a notebook is detached and re attached the temporary view is lost.
-- MAGIC
-- MAGIC **Global temporary view** 
-- MAGIC - is available to all the notebooks in the cluster, if a cluster restarts global temporary view is lost. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Goals:
-- MAGIC - create table **smartphones** and insert sample data
-- MAGIC - Create view above this table using CTAS command
-- MAGIC - finish excercise

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021),
      (11, 'Macbook Air', 'Apple', 2011)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### EXERCISE
-- MAGIC
-- MAGIC - What is the difference between TABLE and VIEW? What are the main advantages and disadvantages?
-- MAGIC - Use **CTAS** syntax to create a view from table **smartphones**, view will contain only Apple devices manufactured in a year 2014 and above
-- MAGIC - use **SHOW TABLES** command and explore results

-- COMMAND ----------

CREATE VIEW view_apple_phones
AS  SELECT * 
    FROM smartphones 
    WHERE brand = 'Apple' and year >= 2014;

-- COMMAND ----------

SELECT * FROM view_apple_phones;

-- COMMAND ----------

-- Notice what will happend if we delete a record from the table
DELETE FROM smartphones where name = 'iPhone 6';
SELECT * FROM view_apple_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- CREATE A TEMP VIEW and select records from it
CREATE TEMP VIEW temp_view_phones_brands
AS  SELECT DISTINCT brand
    FROM smartphones;

SELECT * FROM temp_view_phones_brands;

-- COMMAND ----------

-- detach & re-attach the cluster and try to select from the view
SELECT * FROM temp_view_phones_brands;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- CREATE A GLOBAL TEMP VIEW and select records from it
CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
AS SELECT * FROM smartphones
    WHERE year > 2020
    ORDER BY year DESC;

-- COMMAND ----------

-- GLOBAL TEMP VIEW is not tied to a session but to a cluster
SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

SHOW TABLES IN global_temp;
