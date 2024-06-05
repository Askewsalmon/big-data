import pandas as pd
import numpy as np

df = pd.read_csv("./data/merged.csv")

num_samples = 50000000


def generate_numerical_data(df, columns, num_sample):
    new_data = {}
    for col in columns:
        mean = df[col].mean()
        std = df[col].std()
        new_data[col] = np.random.normal(loc=mean, scale=std, size=num_sample)
    return pd.DataFrame(new_data)


numerical_cols = ["open", "close", "adj_close", "low", "high", "volume"]
new_numerical_data = generate_numerical_data(df, numerical_cols, num_samples)

categorical_cols = ["ticker", "date", "exchange", "name", "sector", "industry"]
new_categorical_data = (
    df[categorical_cols].sample(n=num_samples, replace=True).reset_index(drop=True)
)

new_data = pd.concat([new_categorical_data, new_numerical_data], axis=1)
new_data = new_data[df.columns]

new_data = new_data.fillna("N/A")

new_data.to_csv("data/50000000.csv", index=False)


result = pd.concat([df, new_data], axis=0, ignore_index=True)

result.to_csv("data/merged_50000000.csv", index=False)
