-- Databricks notebook source
-- MAGIC %md
-- MAGIC Goals:
-- MAGIC - create table **employees** and table **employees_salary**
-- MAGIC - insert sample data to these tables
-- MAGIC - explore underlying data
-- MAGIC - finish exercise

-- COMMAND ----------

CREATE OR REPLACE TABLE employees (id INT, name STRING, surname STRING, department STRING);
CREATE OR REPLACE TABLE employee_salary (id INT, salary DECIMAL(10, 2), note STRING);

-- COMMAND ----------

INSERT INTO employees
VALUES 
  (1, "Jon", "Doe", 'Software Development'),
  (2, "Boris", "Zemea", 'Data Analysis'),
  (3, "Mark", "Nash", 'Cybersecurity'),
  (4, "Mia", "Lewiski", 'Testing'),
  (5, "Sarah", "Parker", 'Testing'),
  (6, "Kim", "Chon", 'Software Development'),
  (7, "Alex", "Smith", 'Data Analysis'),
  (8, "Emily", "Johnson", 'Data Analysis'),
  (9, "Michael", "Williams", 'Software Development'),
  (10, "Jessica", "Brown", 'Web Development');

INSERT INTO employee_salary (id, salary, note)
VALUES
  (1, 75000.00, 'Team-Lead'),
  (2, 60000.00, 'Experienced analyst'),
  (3, 85000.00, 'Cybersecurity specialist'),
  (4, 55000.00, 'Entry-level tester'),
  (5, 60000.00, 'Senior tester'),
  (6, 80000.00, 'Senior developer'),
  (7, 45000.00, 'Data analyst - junior'),
  (8, 70000.00, 'Data analyst - senior'),
  (9, 90000.00, 'Lead developer'),
  (10, 70000.00, 'Web development expertise');

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### EXERCISE
-- MAGIC - Explore underlying table files on DBFS
-- MAGIC - which format is used to store data? 
-- MAGIC - read data using spark command and display results

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.read.format("delta").load('dbfs:/user/hive/warehouse/employees'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### EXERCISE
-- MAGIC - Add a new employee to the **employees** table and assign him to **Testing department**
-- MAGIC - Change department name of all employees which are currently assigned to **'Data Analysis'** team to newly established **'Data Science'** department
-- MAGIC - delete record with **id = 8** from both tables
-- MAGIC - use commands **DESCRIBE DETAIL** and **DESCRIBE HISTORY** and explore underlying files stored on DBFS

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'
