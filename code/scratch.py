from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import re
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

df = spark.read.csv("data/chess.csv", header = True)


@F.udf(returnType=StringType())
def stripUDF(x):
   #return x.strip()
   return re.sub(r"\s+", "", x)

for col in df.columns:
    df = df.withColumn(col, (stripUDF(F.trim(col))))
#df.show()

df.select("end_time").show()