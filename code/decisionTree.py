from cleanDataFrame import CleanDataFrame
from typing import List
import pyspark


class DecisionTree():
    def __init__(self, data: pyspark.sql.DataFrame):
        self.data = data


    def getEntropy(self, data: pyspark.sql.DataFrame, varlist: List[str]):
        print(varlist)