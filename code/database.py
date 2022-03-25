import hashlib
import os
import random

import pandas as pd
import psycopg2
import yfinance as yf

from data_utils import reset_purchase_df_index


DATABASE_URL = os.getenv('DATABASE_URL')


def hash_passphrase(passphrase):
    return hashlib.sha256(passphrase.encode('utf-8')).hexdigest()


def generate_random_purchase_data():
    df = pd.DataFrame()
    if os.getenv('ENV') == 'DEV':
        df = pd.read_csv('dev_data.csv', index_col='ticker')
    else:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cur = conn.cursor()
        cur.execute("SELECT * FROM tickers")
        select_data = cur.fetchall()
        df = pd.DataFrame(select_data, columns=['ticker', 'currency', 'type']).set_index('ticker')
        cur.close()
        conn.close()

    def get_random_ticker(type_, df):
        t = (
            df
            .query(f'type == "{type_}"')
        )
        return (
            t.index[random.randint(0, len(t)-1)]
        )

    def append_random_equity(data, df):
        data.append([get_random_ticker('EQUITY', df), random.randint(1, 25), '2020-01-01', 'purchase'])

    def append_random_etf(data, df):
        data.append([get_random_ticker('ETF', df), random.randint(1, 25) * 5, '2020-01-01', 'purchase'])

    def append_random_crypto(data, df):
        data.append([get_random_ticker('CRYPTOCURRENCY', df), random.randint(5, 30) / 10, '2020-01-01', 'purchase'])

    data = []
    for i in range(random.randint(3, 6)):
        append_random_equity(data, df)
    for i in range(random.randint(3, 6)):
        append_random_etf(data, df)
    for i in range(3):
        append_random_crypto(data, df)

    return pd.DataFrame(data, columns=['ticker', 'amount', 'date', 'operation'])


def create_ticker_df_with_currency_and_type(tickers):
    if os.getenv('ENV') == 'DEV':
        return pd.read_csv('dev_data.csv', index_col='ticker')

    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cur = conn.cursor()

    cur.execute("SELECT * FROM tickers")
    select_data = cur.fetchall()
    ticker_df = pd.DataFrame(select_data, columns=['ticker', 'currency', 'type']).set_index('ticker')

    for ticker in tickers:
        if ticker not in ticker_df.index:
            info = yf.Ticker(ticker).info
            cur.execute(
                """
                INSERT INTO tickers (name, currency, quotetype)
                VALUES (%s, %s, %s)
                """, (ticker, info['currency'], info['quoteType'])
            )
            ticker_df.loc[ticker] = {'currency': info['currency'], 'type': info['quoteType']}

    conn.commit()
    cur.close()
    conn.close()
    return ticker_df


def is_ticker_in_db(ticker):
    if os.getenv('ENV') == 'DEV':
        return ticker in pd.read_csv('dev_data.csv').ticker.values

    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cur = conn.cursor()
    cur.execute("SELECT * FROM tickers WHERE name = %s", (ticker.upper(),))
    select_data = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return bool(select_data)


def add_ticker_to_db(ticker, currency, type_):
    if os.getenv('ENV') == 'DEV':
        df = pd.read_csv('dev_data.csv', index_col='ticker')
        df.loc[ticker] = {'currency': currency, 'type': type_}
        df.to_csv('dev_data.csv')
        return

    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cur = conn.cursor()
    cur.execute(
        """
        insert into tickers (name, currency, quotetype)
        values (%s, %s, %s)
        """, (ticker, currency, type_)
    )
    conn.commit()
    cur.close()
    conn.close()


def get_user_purchase_data_from_db(passphrase):
    hash_ = hash_passphrase(passphrase)
    if os.getenv("ENV") == "DEV":
        try:
            return pd.read_pickle('user_purchase_data.p').query(f'hash == "{hash_}"').drop(columns='hash')
        except:
            return pd.DataFrame([], columns=['id', 'ticker', 'amount', 'date', 'operation'])

    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cur = conn.cursor()
    cur.execute("SELECT id, ticker, amount, date, type FROM purchases WHERE hash = %s", (hash_,))
    select_data = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()

    df = (
        pd.DataFrame(select_data, columns=['id', 'ticker', 'amount', 'date', 'operation'])
        .sort_values('id')
        .pipe(reset_purchase_df_index)
    )
    return df


def add_user_purchase_data_to_db(passphrase, data):
    hash_ = hash_passphrase(passphrase)
    if os.getenv("ENV") == "DEV":
        data_copy = data.copy()
        data_copy['hash'] = hash_
        df = pd.DataFrame.from_dict(data_copy, orient='index').T.reset_index(drop=True)
        try:
            df2 = pd.read_pickle('user_purchase_data.p')
            df['id'] = df2.loc[:, 'id'].max() + 1
            df = pd.concat([df2, df])
        except:
            pass
        if df.shape[0] == 1:
            df['id'] = 1
        df = df.pipe(reset_purchase_df_index).to_pickle('user_purchase_data.p')
        return

    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO purchases (hash, ticker, amount, date, type)
        VALUES (%s, %s, %s, %s, %s)
        """, (hash_, data['ticker'], data['amount'], data['date'], data['operation'])
    )
    conn.commit()
    cur.close()
    conn.close()


def delete_user_purchase_data(passphrase, id_):
    hash_ = hash_passphrase(passphrase)
    if os.getenv("ENV") == "DEV":
        df = pd.read_pickle('user_purchase_data.p')
        df.loc[lambda x: x['id'] != id_].to_pickle('user_purchase_data.p')
        return

    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM purchases WHERE hash = %s AND id = %s
        """, (hash_, int(id_))
    )
    conn.commit()
    cur.close()
    conn.close()
