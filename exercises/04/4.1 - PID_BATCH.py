# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC ## PID BATCH
# MAGIC ---
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 8px;">
# MAGIC   <img src="https://raw.githubusercontent.com/animrichter/BDT_2023/master/data/assets/batch.png" style="width: 900">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cíl cvičení: 
# MAGIC - Načíst PID data z json souboru
# MAGIC - Z načtených dat identifikovat atributy (1. , 2. , ..) https://api.golemio.cz/v2/pid/docs/openapi/#/
# MAGIC - Vytvořit tabulky Bronze - Silver
# MAGIC - Gold: Rozdělení podle typu prostředku / průměrné zpoždění / celkové, kolik km ujede jeden bus za den...)
# MAGIC - z Gold tabulek vytvořit přehledný dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC CREATE TABLE AS SELECT
# MAGIC MERGE INTO

# COMMAND ----------

# MAGIC %md
# MAGIC ## TASKS:
# MAGIC ### Think what you need for following tasks? There may be several entries for some vehicle.
# MAGIC ### 1. Find all different types of vehicles in the data
# MAGIC ### 2. Find the average delay for all vehicles in data
# MAGIC ### 3. Find the average delay for every type of vehicle in data
# MAGIC ### 4. Find the average speed for every type of vehicle - is there anything wrong?
# MAGIC ### 5. Find the number of busses, for which their average delay on trip was smaller, then 60 seconds.
# MAGIC
