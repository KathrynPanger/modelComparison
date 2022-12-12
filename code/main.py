from cleanDataFrame import CleanDataFrame
from decisionTree import DecisionTree
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
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
    cleanChess = CleanDataFrame("../data/chess.csv")
    chessDf = cleanChess.data
    chessTree = DecisionTree(chessDf)
    chessTree.getEntropyContinuous(data = chessTree.data, varlist = ["white_rating"])
    #chessTree.data.select("white_rating_pvalue").show()
    #chessTree.data.select("white_rating_entro_p").show()
   # chessTree.data.select("white_rating_entro_p").agg(F.sum("white_rating_entro_p")).show()
   # chessTree.data.select("white_rating_surprise").show()
    print(chessTree.variables)