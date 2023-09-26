# Databricks notebook source
# MAGIC %md
# MAGIC #### Goals:
# MAGIC - set up Azure and Databricks
# MAGIC - Connect to Git repository and pull project
# MAGIC - set up a compute cluster resources (warning: be reasonable with resources and termination time)
# MAGIC   - recommended configuration: Compute -> Create Compute -> Single Node -> runtime: 13.3 **LTS** -> **Uncheck Photon!** -> node: Standard_DS3_v3 -> termination time: **15 min**
# MAGIC    (pridat obrazky)
# MAGIC - attach cluster to the notebook and start it
# MAGIC - go through basic examples bellow
# MAGIC - finish exercise
# MAGIC
# MAGIC

# COMMAND ----------

print("Hello World!")

# COMMAND ----------

message = "Hello Databricks!"
print(message)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world using SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE
# MAGIC - try to use **R** and **Scala** to print out "Hello world!"

# COMMAND ----------

dbutils.help();
dbutils.fs.help();

# COMMAND ----------

# listing folders and files on DBFS
%fs 
ls '/databricks-datasets'

# COMMAND ----------

# Create a DataFrame object from listing folders and files on DBFS
files = dbutils.fs.ls('/databricks-datasets')
display(files)

# COMMAND ----------

# Create DataFrame object from csv file stored on DBFS and display results
df = spark.read.csv("dbfs:/databricks-datasets/flights/departuredelays.csv", header=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXERCISE
# MAGIC - explore data
# MAGIC - find the number of rows in the delayed flights dataset

# COMMAND ----------

# Python recap
my_string = "The content of my_string is quite interesting to analyze"
my_list = my_string.split()
print('String lenght is: ', len(my_string))
print('String contains words: ', my_string.split())
print('String contains unique words: ', set(my_string.split()))
print('String converted to lowercase: ', my_string.lower())
print('Length of the list (number of words) is: ', len(my_list))
print('The first element in the list is: ', my_list[0])
print('The last element in the list is: ', my_list[-1])
