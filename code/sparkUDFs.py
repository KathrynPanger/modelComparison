from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import re

@F.udf(returnType=StringType())
def stripUDF(x):
    #return x.strip()
    return re.sub(r"\s+", "", x)
