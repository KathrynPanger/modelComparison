import polars as pl
from selector import Selector

chessData = pl.read_csv("../data/chess.csv")
selector = Selector(chessData)
test = selector.getEntropyContinuousT("black_rating")
print(test)