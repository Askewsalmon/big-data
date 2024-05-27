import pandas as pd

historical_stock_prices = pd.read_csv("data/historical_stock_prices.csv")
historical_stocks = pd.read_csv("data/historical_stocks.csv")

historical_stocks_filtered = historical_stocks[
    historical_stocks["ticker"].isin(historical_stock_prices["ticker"])
]

historical_stocks_filtered["name"] = historical_stocks_filtered["name"].str.replace(
    r",\s*INC\.$", ".INC.", regex=True
)

historical_stocks_filtered = historical_stocks_filtered.fillna("N/A")


merged_df = pd.merge(
    historical_stock_prices,
    historical_stocks_filtered,
    on="ticker",
    how="inner",
    validate="many_to_one",
)

merged_df.to_csv("data/merged.csv", index=False)
