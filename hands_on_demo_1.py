# Hands on Demo 1 - Big Data Processing 1

!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget https://downloads.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop2.7.tgz
!tar -xvf spark-3.1.3-bin-hadoop2.7.tgz
!pip install -q findspark

# path

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.3-bin-hadoop2.7.tgz"

# FindSpark

import findspark
findspark.init("spark-3.1.3-bin-hadoop2.7")# Spark Home
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# dataframe

df = spark.createDataFrame([{"hello": "world"} for x in range(1000)])
df.show(3)

# psedo_facebook upload

from google.colab import files
files.upload()

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("mode", "failfast").load("pseudo_facebook.csv")

df.show(3)

df.createOrReplaceTempView("facebookdata")

spark.sql("select count(*) from facebookdata").show()

spark.sql("select avg(age), gender from facebookdata group by gender").show()

spark.sql("select avg(friend_count), gender from facebookdata group by gender").show()

spark.sql("select avg(mobile_likes), avg(www_likes) from facebookdata where age >=13 AND age<= 35").show()

spark.sql("select avg(mobile_likes), avg(www_likes) from facebookdata where age> 35").show()

spark.sql("select friend_count, userid from facebookdata order by friend_count Desc").show()

spark.sql("select sum(friend_count) from facebookdata").show()