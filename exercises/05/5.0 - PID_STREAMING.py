# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC ## PID STREAMING
# MAGIC ---
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 20px;">
# MAGIC   <img src="https://raw.githubusercontent.com/animrichter/BDT_2023/master/data/assets/streaming.png" style="width: 1200">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cíl cvičení: 
# MAGIC - Transformovat řešení batch na streaming
# MAGIC - Číst PID data pomocí kafka consumeru
# MAGIC - Z načtených dat identifikovat atributy (1. , 2. , ..) https://api.golemio.cz/v2/pid/docs/openapi/#/
# MAGIC - Vytvořit tabulky Bronze - Silver
# MAGIC - Gold: Rozdělení podle typu prostředku / průměrné zpoždění/celkové, kolik km ujede jeden bus za den...)
# MAGIC - z Gold tabulek vytvořit přehledný dashboard

# COMMAND ----------

bootstrap = "b-3.felpidkafka.vi270o.c2.kafka.eu-central-1.amazonaws.com:9096,b-2.felpidkafka.vi270o.c2.kafka.eu-central-1.amazonaws.com:9096,b-1.felpidkafka.vi270o.c2.kafka.eu-central-1.amazonaws.com:9096"
topic = "FelPidTopic"

raw = (
    spark
        .readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', bootstrap)
        .option('subscribe', topic) 
        .option('startingOffsets', 'earliest')
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="felpiduser" password="";')
        .load()
)

file_path = '/mnt/test/test4.json'
checkpoint = '/mnt/test/check4.txt'

(raw.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", checkpoint)
   .toTable("events")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select string(value), * from events order by `timestamp`

# COMMAND ----------

# MAGIC %md
# MAGIC To visualize geopoints, you can use either: **geopy**, **matplotlib**, **openstreetmaps** or **osmnx** libs.. 
# MAGIC
# MAGIC use command:  %pip install osmnx

# COMMAND ----------

import osmnx as ox

custom_filter='["highway"~"motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|road|road_link"]'
G = ox.graph_from_place("Praha, Czechia", custom_filter=custom_filter)

# COMMAND ----------

import matplotlib.pyplot as plt
# this makes your plot wait and not closing
fig, ax = ox.plot_graph(G, show=False, close=False)
df_geo_p = df_geo.toPandas()
# you can plot all, or some subsection for quicker result
x = df_geo_p.loc[1:300,'x']
y = df_geo_p.loc[1:300,'y']

ax.scatter(x, y, c='red')

plt.show()
