import pandas as pd
import streamlit as st
import yfinance as yf


@st.experimental_memo(max_entries=1000, show_spinner=False)
def get_asset_splits(ticker, cache_date):
    return yf.Ticker(ticker).actions.loc[:, 'Stock Splits']


@st.experimental_memo(max_entries=50, show_spinner=False)
def get_historical_prices(tickers, start, cache_date):
    return yf.download(tickers, start=start).loc[:, 'Close']


def correct_asset_amount_affected_by_split(df: pd.DataFrame, ticker_types: pd.Series):
    df = df.copy()
    for ticker in df.ticker.unique():
        if ticker_types[ticker] == 'CRYPTO':
            continue
        # there can only be one split a day, so cache_date ensures we only download split data once a day
        splits = get_asset_splits(ticker, cache_date=str(pd.Timestamp.now().date()))
        for date, split in splits.iteritems():
            if split == 0:
                continue
            df.loc[
                lambda x: (pd.to_datetime(x.date) <= date) & (x.ticker == ticker),
                'amount'
            ] *= split
    return df


def resample(df, freq):
    time_format = '%Y-%m' if freq == 'M' else '%Y-%m-%d'
    return (
        df
        .reset_index()
        .groupby(pd.Grouper(key='Date', freq=freq))
        .mean()
        .reset_index()
        .assign(Date=lambda x: x.Date.apply(lambda x: x.strftime(time_format)))
        .set_index('Date')
    )


def calculate_historical_value(s, purchase_df):
    if s.name not in purchase_df.ticker.unique():
        return s

    ticker_purchase_df = (
        purchase_df
        .assign(date=lambda x: pd.to_datetime(x.date))
        .query(f'ticker == "{s.name}"')
        .sort_values('date')
    )

    output = s * 0
    for index, row in ticker_purchase_df.iterrows():
        if row['operation'] == 'purchase':
            output.loc[row['date']:] += s.loc[row['date']:] * row['amount']
        elif row['operation'] == 'sale':
            output.loc[row['date']:] -= s.loc[row['date']:] * row['amount']
        else:
            raise ValueError('unexpected operation')

    return output


def calculate_current_assets_from_purchases_and_sales(purchase_df, ticker_info_df):
    return (
        purchase_df
        .groupby('ticker')
        .apply(lambda x: x.query('operation == "purchase"').amount.sum() - x.query('operation == "sale"').amount.sum())
        .to_frame()
        .rename(columns={0: 'amount'})
        .join(ticker_info_df, how='left')
        .assign(type=lambda x: x.type.str.replace('CRYPTOCURRENCY', 'CRYPTO'))
    )


def add_latest_asset_prices(df, historical_prices):
    latest_prices = historical_prices.ffill().iloc[-1]
    latest_currency_prices_in_usd = (
        latest_prices
        .loc[lambda x: x.index.str.endswith('USD=X')]
        .rename(lambda x: x.split('USD=X')[0])
    )
    return (
        df
        .assign(
            price=latest_prices,
            currency_rate=lambda x: x.currency.map(latest_currency_prices_in_usd.to_dict()),
            total_usd=lambda x: x.currency_rate * x.amount * x.price,
            total_pln=lambda x: x.total_usd / latest_currency_prices_in_usd['PLN'],
        )
        .sort_values(['type', 'total_pln'], ascending=False)
        .round(2)
    )


def calculate_historical_value_in_pln(historical_prices, purchase_df, assets_df, months_n=None, frequency='D'):
    if months_n:
        historical_prices = historical_prices.loc[pd.Timestamp.now() - pd.Timedelta(months_n * 4, unit='W'):]

    historical_prices = (
        historical_prices
        .apply(calculate_historical_value, purchase_df=purchase_df)
        .pipe(resample, freq=frequency)
    )
    historical_currencies_in_usd = (
        historical_prices
        .loc[:, lambda x: x.columns.str.endswith('USD=X')]
        .rename(columns=lambda x: x.split('USD=X')[0])
        .assign(USD=1)
    )
    return (
        historical_prices
        .loc[:, lambda x: ~x.columns.str.endswith('USD=X')]
        .apply(
            lambda x:
            (
                x *
                historical_currencies_in_usd.loc[:, assets_df.loc[x.name, 'currency']]
            ) /
            historical_currencies_in_usd.loc[:, 'PLN']
        )
        .loc[:, assets_df.sort_values(['total_pln']).index]
    )


def reset_purchase_df_index(df):
    df = df.copy().reset_index(drop=True)
    df.index += 1
    return df
