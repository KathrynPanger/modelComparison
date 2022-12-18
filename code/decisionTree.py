from cleanDataFrame import CleanDataFrame
from typing import List
import pyspark
import math
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from scipy import stats
from variable import Variable



class DecisionTree():
    def __init__(self, data: pyspark.sql.DataFrame):
        self.data = data
        self.n = self.data.select(data.columns[0]).count()
        self.variables = {}

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
                sampleMean = float(data.select(col).agg(F.mean(col)).collect()[0][0])
                sampleStdDev = float(data.select(col).agg(F.stddev(col)).collect()[0][0])
                sampleN = self.n
                # standardError = sampleStdDev/sampleN**(1/2)
                degreesOfFreedom = sampleN-1
                #create a pyspark function to get probability of each observation
                #given sample mean
                @F.udf(returnType=FloatType())
                def tDistProbability(observedValue):
                    if observedValue is not None:
                        observedValue = float(observedValue)
                        #t = (observedValue - sampleMean) / standardError
                        # Using the standard error didn't work because I got all
                        # zeros, or values very close to it.
                        # I need the p-values to sum to 1, so
                        # I'm going to find the proportion of the sum of all the p
                        # values each p-value I got is. This will make values
                        # which are more extreme proportionately more surprising than values
                        # which are less extreme.

                        t = (observedValue - sampleMean) / sampleStdDev
                        p = float(stats.t.sf(abs(t), degreesOfFreedom))
                        return p
                    else:
                        return None

                self.data = self.data.withColumn(f"{col}_pvalue", tDistProbability(col))
                columnSum = self.data.select(col).agg(F.sum(col)).collect()[0][0]
                @F.udf(returnType=FloatType())
                def proportionOfTotal(observedValue):
                    if observedValue is not None:
                        return observedValue/columnSum
                    else:
                        return None
                self.data = self.data.withColumn(f"{col}_entro_p", proportionOfTotal(f"{col}_pvalue"))
                #Ha! Ya Get it?! Entro "p"?
                # No? Okay.

                #find the surprise of each observation
                @F.udf(returnType=FloatType())
                def pToSurprise(observedValue):
                    if observedValue is not None:
                        return observedValue * math.log(1/observedValue, 10)
                    else:
                        return None
                self.data = self.data.withColumn(f"{col}_surprise", pToSurprise(f"{col}_entro_p"))
                entropy = self.data.select(f"{col}_surprise").agg(F.sum(f"{col}_surprise")).collect()[0][0]
                variable = Variable(col)
                variable.entropy = entropy
                self.variables[col] = variable

    def getEntropyDiscrete(self, data: pyspark.sql.DataFrame, varlist: List[str]):
        pass
    def getEntropyDiscrete2(self, data):
        pass