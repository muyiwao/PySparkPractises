# Hands-on-demo 2

!apt-get install openjdk-8-jdk-headless > /dev/null
!wget -q https://mirrors.estointernet,in/apache/spark/spark=301/spark-3.0.1-bin-hadoop2.7.tgz
!tar xf spark-3.0.1-bin-hadoop2.7.tgz
!pip install -q findspark

# path

import os 
os.environ["JAVA_HOME"] = "version and path of java"
os.environ["SPARK_HOME"] = " version and path of hadoop"

# FindSpark

import findspark
findspark.init()
from pyspark.sql import sparksession
spark = sparksession.builder.master(machine[*]).getOrCreate()

from google.colab import files
files.upload()

dataframe = spark.read.format("csv").options("header",true).option("inferSchema", true).option("mode","failfast").load("location of data")

df.createOrReplaceTempView("table") - creates table virtually 

spark.sql("select * from data").show()

spark.sql("select count(*) from data").show()

select("select avg(data) from data").show()

group by
spark.sql("select avg(data), otherdata from data group by otherdata) 

store variable
x = select("select avg(data) from data").collect()[0][0]

spark.sql("select avg(data), as data1, otherdata from data group by otherdata order by data1") 


