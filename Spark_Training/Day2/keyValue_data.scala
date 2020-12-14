// Databricks notebook source
val data =List("shikha","singh","shikha")

// COMMAND ----------

val datardd=sc.parallelize(data)

// COMMAND ----------

datardd.countByValue

// COMMAND ----------

val data2=List(1,2,3,4,5)
val data2rdd=sc.parallelize(data2)

// COMMAND ----------

val productrdd=data2rdd.reduce((x,y)=>x*y)

// COMMAND ----------

// DBTITLE 1,NumberDataRDD
val primerdd=sc.textFile("/FileStore/tables/numberData.csv")

// COMMAND ----------

// DBTITLE 1,Removing header and 0
val res=primerdd.filter(x=>(x!="Number") && (x!="0"))
val n=res.count


// COMMAND ----------

// DBTITLE 1,Prime number function
def prime(x:Int): Boolean ={
  if(x<=1) false
  else !(2 until (x-1)).exists(i=>x%i==0)
}

// COMMAND ----------

val res1=res.filter(x=>prime(x.toInt)).map(x=>x.toInt)

// COMMAND ----------

val sum=res1.reduce((x,y) => x+y)
sum
