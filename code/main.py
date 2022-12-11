from cleanDataFrame import CleanDataFrame
from decisionTree import DecisionTree
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
import re

#set up spark
spark = SparkSession.builder.appName("sparkSession").getOrCreate()

#UDFs
#####

## Trim headers and rows
@F.udf(returnType=StringType())
def stripUDF(x):
   #return x.strip()
   return re.sub(r"\s+", "", x)


if __name__ == "__main__":
    chessDf = CleanDataFrame("../data/chess.csv")
    chessDf.data.select("end_time").show()
    chessTree = DecisionTree(chessDf)
    chessTree.getEntropy(data = chessDf.data, varlist = [col for col in chessDf.data.columns])