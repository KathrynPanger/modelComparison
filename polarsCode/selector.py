import polars as pl
class Selector():
    def __init__(self, data: pl.DataFrame):
        self.data = data

    def getEntropyContinuousT(self, columnName: str):
        variable = self.data.select(pl.col(columnName))
        sampleMean = variable.mean()[0,0]
        sampleStandardDev = variable.std()[0,0]
        return sampleStandardDev
    def getEntropyContinuousBins(self):
        pass
    def getEntropyDiscreet(self):
        pass
    def getSplitabilityScore(self):
        pass