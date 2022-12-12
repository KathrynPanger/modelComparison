from cleanDataFrame import CleanDataFrame
from typing import List
import pyspark
from pyspark.sql import functions as F
from scipy import stats

class DecisionTree():
    def __init__(self, data: pyspark.sql.DataFrame):
        self.data = data
        self.n = self.data.select(data.columns[0]).count()

    def getEntropyContinuous(self,
                             data: pyspark.sql.DataFrame,
                             varlist: List[str]):
        # This method treats our sample mean as an estimation of the
        # true population mean.
        # First, we get the t-statistic associated with each observation's
        # distance from the sample mean. The p-value associated with this
        # t-statistic is used to estimate surprise for that observation.
        # Surprise for all observations are summed to estimate entropy for the
        # whole variable.

        for col in data.columns:
            if col in varlist:
                #get the sample mean
                sampleMean = data.select(col).agg(F.mean(col)).collect()[0][0]
                sampleStdDev = data.select(col).agg(F.stddev(col)).collect()[0][0]
                sampleN = self.n
                print(sampleStdDev)

                standardError = sampleStdDev/sampleN**(1/2)
                degreesOfFreedom = n-1
                #create a pyspark function to get probability of each observation
                #given sample mean
                @F.udf(returnType=FloatType())
                def tDistProbability(observedValue):
                    t = (observedValue - sampleMean) / standardError
                    p = stats.t.sf(abs(t), degreesOfFreedom)
                    return p
                data = data.withColumn(f"{col}_pvalue")


    def getEntropyDiscrete(self, data: pyspark.sql.DataFrame, varlist: List[str]):
        pass