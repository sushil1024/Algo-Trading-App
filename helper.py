import time
import datetime
import pandas as pd

epoch_to_datetime = lambda epoch: datetime.datetime.fromtimestamp(epoch)


def tick_to_df(tick, symbol):
    df = pd.DataFrame.from_dict(tick[symbol])
    df = df.sort_values(by='timestamp')
    df = df.drop(index=df.index[0])  # drop the first row

    df.set_index('timestamp', inplace=True)

    df = df.resample('min').agg(
        open=('c', 'first'),
        high=('c', 'max'),
        low=('c', 'min'),
        close=('c', 'last')
    ).dropna()

    return df


