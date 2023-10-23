-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## How to SQL query a file
-- MAGIC <br/>
-- MAGIC
-- MAGIC - The goal is to demonstrate how to use SQL to execute queries directly over files (json, csv, text, ..)
-- MAGIC - List and explore all files within the **data/customers-json** and **data/books-csv** folders  (use **dbutils.fs** command)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # replace <username> with your databricks repository name.
-- MAGIC # "file:" prefix and absolute file path are required for PySpark
-- MAGIC repo_user = "<username>"
-- MAGIC repo_abs_path = f"file:/Workspace/Repos/{repo_user}/BDT_2023/"
-- MAGIC
-- MAGIC # to be able to use repo_abs_path as a parameter in a SQL query, we need to set it up as a follows:
-- MAGIC spark.conf.set('repo.abs_path', f'{repo_abs_path}')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # list all files within a folder
-- MAGIC
-- MAGIC files_customers = dbutils.fs.ls(f"{repo_abs_path}/data/customers-json")
-- MAGIC files_books = dbutils.fs.ls(f"{repo_abs_path}/data/books-csv")
-- MAGIC
-- MAGIC display(files_customers)
-- MAGIC display(files_books)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### EXERCISE
-- MAGIC <br/>
-- MAGIC
-- MAGIC - Go through the examples below to explore how to query directly over files.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 8px; padding-right: 200px">
-- MAGIC   <img src="https://raw.githubusercontent.com/animrichter/BDT_2023/master/data/assets/querying_files.png" style="width: 900">
-- MAGIC </div>

-- COMMAND ----------

-- select all from within export_001.json file

SELECT * FROM json.`${repo.abs_path}/data/customers-json/export_001.json`

-- COMMAND ----------

-- select all from files which starts with export_ from within customers-json folder 

SELECT * FROM json.`${repo.abs_path}/data/customers-json/export_*.json`

-- COMMAND ----------

-- count all records from all files within the customers-json folder 

SELECT count(*) FROM json.`${repo.abs_path}/data/customers-json`

-- COMMAND ----------

-- select all from within customers-json folder and notice Profile column
-- we can also obtain record origin (file name and path) adding input_file_name() to the select statement

SELECT input_file_name() source_file, * FROM json.`${repo.abs_path}/data/customers-json`

-- COMMAND ----------

-- SELECT profile column from the customers table and explore it´s format

SELECT profile FROM json.`${repo.abs_path}/data/customers-json`

-- COMMAND ----------

-- use get_json_object to get a value from json column

SELECT customer_id, 
       get_json_object(profile, '$.first_name') first_name, 
       get_json_object(profile, '$.last_name') last_name,
       get_json_object(profile, '$.gender') gender,
       get_json_object(profile, '$.address.city') city,
       email, 
       updated 
FROM json.`${repo.abs_path}/data/customers-json/export_001.json`

-- COMMAND ----------

-- You can 'read' data in various formats in SQL SELECT statement (SELECT * FROM <FORMAT>.<PATH>)
-- examples:

-- SELECT * FROM text.`${repo.abs_path}/data/customers-json`;
-- SELECT * FROM binaryFile.`${repo.abs_path}/data/customers-json`;
SELECT * FROM csv.`${repo.abs_path}/data/books-csv` 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### How to create a table from files: 
-- MAGIC
-- MAGIC documentation: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table.html
-- MAGIC
-- MAGIC <br/>
-- MAGIC
-- MAGIC - CREATE TABLE USING
-- MAGIC - CREATE TABLE AS SELECT

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### EXERCISE
-- MAGIC <br/>
-- MAGIC
-- MAGIC - Create table table **BOOKS** with use of **CREATE TABLE** *tbl_name* (*column_name* *data_type*, ..) **USING** command
-- MAGIC    - **schema:** book_id STRING, title STRING, author STRING, category STRING, price DOUBLE
-- MAGIC    - location of table files: **'${repo.abs_path}/data/books-csv'**
-- MAGIC
-- MAGIC - What is the total price of all books in 'Computer Science' cathegory?
-- MAGIC
-- MAGIC - Create table table **CUSTOMERS** with columns: customer_id, first_name, last_name, email, street, city and country

-- COMMAND ----------

-- create a books table:

-- COMMAND ----------

-- create a customers table:
