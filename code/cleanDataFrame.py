from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import re
spark = SparkSession.builder.appName("sparkSession").getOrCreate()

class CleanDataFrame:
    def __init__ (self, csv):
        self.csv = csv
        self.data = spark.read.csv(csv, header = True)

        @F.udf(returnType=StringType())
        def stripUDF(x):
            #return x.strip()
            return re.sub(r"\s+", "", x)

        for col in self.data.columns:
            self.data = self.data.withColumn(col, (stripUDF(F.trim(col))))
#df.show()



if __name__ == "__main__":
    chessDf = CleanDataFrame("../data/chess.csv")
    chessDf.data.select("end_time").show()