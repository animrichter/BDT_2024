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
# MAGIC - Gold: Rozdělení podle typu prostředku / průměrné zpoždění / celkové, kolik km ujede jeden bus za den...)
# MAGIC - z Gold tabulek vytvořit přehledný dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Fields
# MAGIC
# MAGIC ### 1. `geometry.coordinates`
# MAGIC - **Description**: Coordinates representing the location of the vehicle.
# MAGIC
# MAGIC ### 2. `properties.trip.vehicle_type.description_en`
# MAGIC - **Description**: Description of the vehicle type in English.
# MAGIC   - **Note**: If empty, it represents a train.
# MAGIC
# MAGIC ### 3. `properties.trip.agency_name.scheduled`
# MAGIC - **Description**: Agency name that currently operates the trip
# MAGIC
# MAGIC ### 4. `properties.trip.gtfs.trip_id`
# MAGIC - **Description**: Identifier of the trip in the GTFS Static feed.
# MAGIC
# MAGIC ### 5. `properties.trip.vehicle_registration_number`
# MAGIC - **Description**: Four-digit identifier of the vehicle in the system.
# MAGIC
# MAGIC ### 6. `properties.trip.gtfs.route_short_name`
# MAGIC - **Description**: Identification of the line used for the public.
# MAGIC
# MAGIC ### 7. `properties.last_position.delay.actual`
# MAGIC - **Description**: Current delay, in seconds.
# MAGIC
# MAGIC ### 8. `properties.last_position.origin_timestamp`
# MAGIC - **Description**: Time at which the position was sent from the vehicle (UTC).
# MAGIC
# MAGIC ### 9. `properties.last_position.shape_dist_traveled`
# MAGIC - **Description**: Number of kilometers traveled on the route.

# COMMAND ----------

kafka_cluster = "b-3-public.felkafkamsk.56s6v1.c2.kafka.eu-central-1.amazonaws.com:9196,b-2-public.felkafkamsk.56s6v1.c2.kafka.eu-central-1.amazonaws.com:9196,b-1-public.felkafkamsk.56s6v1.c2.kafka.eu-central-1.amazonaws.com:9196"

topic = "fel-pid-topic"

# COMMAND ----------

schema = 'array<struct<geometry: struct<coordinates: array<double>, type: string>, properties: struct<last_position: struct<bearing: int, delay: struct<actual: int, last_stop_arrival: int, last_stop_departure: int>, is_canceled: string, last_stop: struct<arrival_time: string, departure_time: string, id: string, sequence: int>, next_stop: struct<arrival_time: string, departure_time: string, id: string, sequence: int>, origin_timestamp: string, shape_dist_traveled: string, speed: string, state_position: string, tracking: boolean>, trip: struct<agency_name: struct<real: string, scheduled: string>, cis: struct<line_id: string, trip_number: string>, gtfs: struct<route_id: string, route_short_name: string, route_type: int, trip_headsign: string, trip_id: string, trip_short_name: string>, origin_route_name: string, sequence_id: int, start_timestamp: string, vehicle_registration_number: string, vehicle_type: struct<description_cs: string, description_en: string, id: int>, wheelchair_accessible: boolean, air_conditioned: boolean, usb_chargers: boolean>>, type: string>>'

# COMMAND ----------


raw = (
    spark
        .readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', kafka_cluster)
        .option('subscribe', topic)
        .option('startingOffsets', "earliest")
        .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.jaas.config', 'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="FELPIDUSER" password="";')
        .load()
)

checkpoint = '/mnt/pid/checkpoint_file.txt'

(raw.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", checkpoint)
   .toTable("fel_pid_topic_data")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select string(value), * from fel_pid_topic_data order by `timestamp` desc

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC =======================================================================================================================================
# MAGIC
# MAGIC Zadání A: Proveďte analýzu zpoždění různých typů dopravních prostředků (autobusy, tramvaje, metro, vlaky) v průběhu celého dne. Porovnejte průměrné zpoždění pro každý typ vozidla a zjistěte, jak se mění v 2 hod. časových intervalech. Výsledky zobrazte v grafu. Jako další krok identifikujte 3 místa s největším průměrným zpožděním pro daný typ dopravního prostředku a vizualizujte je na mapě.
# MAGIC Cíl: Odhalit vzorce v zpoždění různých typů dopravních prostředků a lokalizovat klíčová místa, kde k nim dochází.
# MAGIC
# MAGIC ============================================================================================================================
# MAGIC 
# MAGIC Zadání B: Proveďte analýzu zpoždění různých typů dopravních prostředků (autobusy, tramvaje, metro, vlaky) v průběhu celého dne. Porovnejte průměrné zpoždění pro každý typ vozidla a zjistěte, jak se mění v 3 hod. časových intervalech. Výsledky zobrazte v grafu. Jako další krok identifikujte 3 místa s nejmenším průměrným zpožděním pro daný typ dopravního prostředku a vizualizujte je na mapě.
# MAGIC Cíl: Odhalit vzorce v zpoždění různých typů dopravních prostředků a lokalizovat klíčová místa, kde k nim dochází.
# MAGIC
# MAGIC =============================================================================================================================
