import math

import polars as pl
from scipy import stats


class Selector():
    def __init__(self, data: pl.DataFrame):
        self.data = data
        self.n = self.data.select(pl.count())[0,0]

    def getEntropyContinuousT(self, columnName: str):
        originalVariable = self.data.select(pl.col(columnName))
        sampleMean = originalVariable.mean()[0,0]
        sampleStdDev = originalVariable.std()[0, 0]
        degreesOfFreedom = self.n - 1
        # standardError = sampleStdDev / self.n ** (1 / 2)
        # tValues = variable.with_column(((pl.col(columnName) - sampleMean) / standardError).alias("t_values"))
        tValues = originalVariable.with_column(((pl.col(columnName) - sampleMean) / sampleStdDev).alias("t_values"))
        pValues = tValues.with_column(pl.col("t_values").apply(lambda x: stats.t.sf(x, degreesOfFreedom)).alias("p_values"))
        pSum = pValues.sum()[0,0]
        proportionOfTotal = pValues.with_column(pl.col("p_values").apply(lambda x: x / pSum).alias("proportion_prob"))
        surprise = proportionOfTotal.with_column(pl.col("proportion_prob").apply(lambda x: math.log(1/x, 2)).alias("surprise"))
        entropy = surprise.sum()[0,0]
        return entropy

    def getEntropyContinuousBins(self):
        pass
    def getEntropyDiscreet(self, columnName):
        counts = self.data.select(pl.col(columnName)).groupby(pl.col(columnName)).agg(pl.count().alias("counts"))
        probabilityOfSelection = counts.with_column((pl.col("counts")/self.n).alias("probability_of_selection"))
        surprise = probabilityOfSelection.with_column(pl.col("probability_of_selection").apply(lambda x: math.log(1/x, 2)).alias("surprise"))
        entropy = surprise.select("surprise").sum()[0,0]
        return entropy

    def getSplitStats(self):
        pass
    #test
