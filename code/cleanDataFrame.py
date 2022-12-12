import sparkUDFs
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType

import re

#start spark
spark = SparkSession.builder.appName("sparkSession").getOrCreate()

# Create all UDFs for later
@F.udf(returnType=StringType())
def stripUDF(x):
   #return x.strip()
   return re.sub(r"\s+", "", x)


#Dataframe Class

class CleanDataFrame:
    def __init__ (self, csv):
        self.csv = csv
        self.data = spark.read.csv(csv, header = True)

        for col in self.data.columns:
            self.data = self.data.withColumn(col, (stripUDF(F.trim(col))))


if __name__ == "__main__":
    pass
    #chessDf = CleanDataFrame("../data/chess.csv")
    #chessDf.data.select("end_time").show()