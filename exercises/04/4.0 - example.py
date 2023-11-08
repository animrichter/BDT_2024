# Databricks notebook source
dataPath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"

diamonds = sqlContext.read.format("csv") \
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load(dataPath)

display(diamonds)

# COMMAND ----------

# first we group by two variables, cut and color and then compute the average price. 
# Then we're going to inner join that to the original dataset on the column color. 
# Then we'll select the average price as well as the carat from that new dataset.
df1 = diamonds.groupBy("cut", "color").avg("price") # a simple grouping

df2 = df1\
  .join(diamonds, on='color', how='inner')\
  .select("`avg(price)`", "carat")
# a simple join and selecting some columns

# COMMAND ----------

# MAGIC %md
# MAGIC These transformations are now complete in a sense but nothing has happened. As you'll see above we don't get any results back!
# MAGIC
# MAGIC The reason for that is these computations are lazy in order to build up the entire flow of data from start to finish required by the user. This is a intelligent optimization for two key reasons. Any calculation can be recomputed from the very source data allowing Apache Spark to handle any failures that occur along the way, successfully handle stragglers. Secondly, Apache Spark can optimize computation so that data and computation can be pipelined as we mentioned above. Therefore, with each transformation Apache Spark creates a plan for how it will perform this work.
# MAGIC
# MAGIC To get a sense for what this plan consists of, we can use the explain method. Remember that none of our computations have been executed yet, so all this explain method does is tell us the lineage for how to compute this exact dataset.

# COMMAND ----------

df2.explain()

# COMMAND ----------

df2.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Click the little arrow next to where it says **Spark Jobs** after that cell finishes executing and then click the View link, followed by the DAG Visualization button. This brings up the Apache Spark Web UI right inside of your notebook. This can also be accessed from the cluster attach button at the top of this notebook. In the Spark UI, you should see something that includes a diagram something like this.
# MAGIC
# MAGIC This is the Directed Acyclic Graphs (DAG) of all the computations that have to be performed in order to get to that result. The visualization shows us all the steps that Spark has to get our data into the final form.
# MAGIC
# MAGIC Again, this DAG is generated because transformations are lazy - while generating this series of steps Spark will optimize lots of things along the way and will even generate code to do so. This is one of the core reasons that users should be focusing on using DataFrames and Datasets instead of the legacy RDD API. With DataFrames and Datasets, Apache Spark will work under the hood to optimize the entire query plan and pipeline entire steps together. You'll see instances of WholeStageCodeGen as well as tungsten in the plans and these are a part of the improvements in Spark SQL, which you can read more about on the Databricks blog.
# MAGIC
# MAGIC In this diagram you can see that we start with a CSV all the way on the left side, perform some changes, merge it with another CSV file (that we created from the original DataFrame), then join those together and finally perform some aggregations until we get our final result!
