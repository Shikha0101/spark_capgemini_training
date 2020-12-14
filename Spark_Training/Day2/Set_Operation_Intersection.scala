// Databricks notebook source
val julyrdd=sc.textFile("/FileStore/tables/nasa_july.tsv")

// COMMAND ----------

val augustrdd=sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

val res1=julyrdd.map(x=>x.split("\t")(0)).filter(x=>x!="host")
val res2=augustrdd.map(x=>x.split("\t")(0)).filter(x=>x!="host")
val inter_rdd=res1.intersection(res2)
inter_rdd.count
