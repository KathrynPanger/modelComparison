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
        tValues = (variable - sampleMean) / standardError
        proportionOfTotal = tValues / tValues.sum()[0,0]
        #surprise = proportionOfTotal.apply(lambda x: [math.log(1/item, 2) for item in x])

        # tValues = (variable - sampleMean) / sampleStdDev
        # pValues = tValues.apply(lambda x: [float(stats.t.sf(abs(item), degreesOfFreedom)) for item in x])
        # pValues = variable.apply(lambda x: [float(stats.t.sf(abs((item - sampleMean) / sampleStdDev), degreesOfFreedom)) for item in x])
        return proportionOfTotal



    def getEntropyContinuousBins(self):
        pass
    def getEntropyDiscreet(self):
        pass
    def getSplitabilityScore(self):
        pass
