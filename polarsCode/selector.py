import math

import polars as pl
from scipy import stats


class Selector():
    def __init__(self, data: pl.DataFrame):
        self.data = data
        self.n = self.data.select(pl.count())[0,0]

    def getEntropyContinuousT(self, columnName: str):
        variable = self.data.select(pl.col(columnName))
        sampleMean = variable.mean()[0,0]
        sampleStdDev = variable.std()[0, 0]
        standardError = sampleStdDev/self.n**(1/2)
        degreesOfFreedom = self.n - 1
        # tValues = variable.with_column(((pl.col(columnName) - sampleMean) / standardError).alias("t_values"))
        tValues = variable.with_column(((pl.col(columnName) - sampleMean) / sampleStdDev).alias("t_values"))
        pValues = tValues.with_column(pl.col("t_values").apply(lambda x: stats.t.sf(x, degreesOfFreedom)).alias("p_values"))
        pSum = pValues.sum()[0,0]
        proportionOfTotal = pValues.with_column(pl.col("p_values").apply(lambda x: x / pSum).alias("proportion_prob"))
        surprise = proportionOfTotal.with_column(pl.col("proportion_prob").apply(lambda x: math.log(1/x, 2)).alias("surprise"))

        # tValues = (variable - sampleMean) / sampleStdDev
        # pValues = tValues.apply(lambda x: [float(stats.t.sf(abs(item), degreesOfFreedom)) for item in x])
        # pValues = variable.apply(lambda x: [float(stats.t.sf(abs((item - sampleMean) / sampleStdDev), degreesOfFreedom)) for item in x])
        return surprise



    def getEntropyContinuousBins(self):
        pass
    def getEntropyDiscreet(self):
        pass
    def getSplitabilityScore(self):
        pass
