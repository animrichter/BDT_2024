-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## UDF
-- MAGIC <br/>
-- MAGIC
-- MAGIC - documentation: https://docs.databricks.com/en/udf/index.html
-- MAGIC - create a custom UDF function to return domain part of the email address

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_email_domain(email STRING)
RETURNS STRING

RETURN split(email, "@")[1]

-- COMMAND ----------

-- query table customers created in the previous notebook
SELECT email, get_email_domain(email) domain FROM customers

-- COMMAND ----------

DESCRIBE FUNCTION get_email_domain;

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_email_domain

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE "Unknown"
       END;

-- COMMAND ----------

DROP FUNCTION get_email_domain;
DROP FUNCTION site_type;
