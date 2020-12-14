// Databricks notebook source
val julyrdd=sc.textFile("/FileStore/tables/nasa_july.tsv")

// COMMAND ----------

val augustrdd=sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

val unionrdd=julyrdd.union(augustrdd)
unionrdd.take(3)

// COMMAND ----------

val header=unionrdd.first

// COMMAND ----------

// DBTITLE 1,1st Approach
unionrdd.filter(x=>x!=header).take(2)

// COMMAND ----------

// DBTITLE 1,2nd Approach
def headerRemover(x: String): Boolean= !(x.contains("bytes"))


// COMMAND ----------

val finalrdd=unionrdd.filter(x=>headerRemover(x))
finalrdd.take(2)

// COMMAND ----------

finalrdd.count

// COMMAND ----------

// DBTITLE 1,Sample on the data
finalrdd.sample(withReplacement=true, fraction=0.20).collect()
