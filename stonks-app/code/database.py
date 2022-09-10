import hashlib
import os
import random

import firebase_admin
import pandas as pd
import yfinance as yf
from firebase_admin import firestore
from google.auth import credentials

from data_utils import reset_purchase_df_index


def hash_passphrase(passphrase):
    return hashlib.sha256(passphrase.encode('utf-8')).hexdigest()


def initialize_firestore():
    # don't initialize twice
    if len(firebase_admin._apps) > 0:
        return
    firebase_admin.initialize_app()


def get_firestore_client():
    initialize_firestore()
    if os.getenv('FIRESTORE_EMULATOR_HOST'):
        return firestore.Client(
            project=os.getenv('FIRESTORE_PROJECT_ID'),
            credentials=credentials.AnonymousCredentials(),
        )
    return firestore.client()


def generate_random_purchase_data():
    df = pd.DataFrame()
    db = get_firestore_client()
    df = read_ticker_df_from_firestore()

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


def write_dataframe_to_firestore(db: firestore.Client, collection_name: str, df: pd.DataFrame) -> None:
    data = df.to_dict('records')
    for d in data:
        doc_ref = db.collection(collection_name).document()
        doc_ref.set(d)


def write_dict_to_firestore(db: firestore.Client, collection_name: str, d: dict) -> None:
    doc_ref = db.collection(collection_name).document()
    doc_ref.set(d)


def read_dataframe_from_firestore(db: firestore.Client, collection_name: str) -> pd.DataFrame:
    docs = db.collection(collection_name).stream()
    data = [doc.to_dict() for doc in docs]
    df = pd.DataFrame(data)
    return df


def query_firestore(db: firestore.Client, collection_name: str, field: str, operator: str, value) -> pd.DataFrame:
    docs = db.collection(collection_name).where(field, operator, value).stream()
    data = [doc.to_dict() for doc in docs]
    df = pd.DataFrame(data)
    return df


def is_ticker_in_db(ticker: str) -> bool:
    db = get_firestore_client()
    df = query_firestore(db, 'tickers', 'name', '==', ticker)
    return not df.empty


def read_ticker_df_from_firestore() -> pd.DataFrame:
    db = get_firestore_client()
    df = read_dataframe_from_firestore(db, 'tickers')
    if df.empty:
        # dummy data
        df = pd.DataFrame(
            data=[
                {'quotetype': 'EQUITY', 'name': 'AAPL', 'currency': 'USD'},
                {'quotetype': 'ETF', 'name': 'SPY', 'currency': 'USD'},
                {'quotetype': 'CRYPTOCURRENCY', 'name': 'BTC-USD', 'currency': 'USD'},
            ]
        )
        write_dataframe_to_firestore(db, 'tickers', df)
        df = read_dataframe_from_firestore(db, 'tickers')
    df = (
        df
        .rename(columns={'name': 'ticker', 'quotetype': 'type'})
        .set_index('ticker')
    )

    return df


def create_ticker_df_with_currency_and_type(tickers: list) -> pd.DataFrame:
    db = get_firestore_client()
    ticker_df = read_ticker_df_from_firestore()
    for ticker in tickers:
        if ticker not in ticker_df.index:
            info = yf.Ticker(ticker).info
            data = {
                'name': ticker,
                'currency': info['currency'],
                'quotetype': info['quoteType']
            }
            write_dict_to_firestore(db, 'tickers', data)
            ticker_df.loc[ticker] = {'currency': info['currency'], 'type': info['quoteType']}

    return ticker_df


def add_ticker_to_db(ticker: str, currency: str, type_: str):
    db = get_firestore_client()
    data = {
        'name': ticker,
        'currency': currency,
        'quotetype': type_
    }
    write_dict_to_firestore(db, 'tickers', data)


def get_user_purchase_data_from_db(passphrase):
    hash_ = hash_passphrase(passphrase)
    db = get_firestore_client()
    df = query_firestore(db, 'purchases', 'hash', '==', hash_)
    if df.empty:
        return pd.DataFrame([], columns=['id', 'ticker', 'amount', 'date', 'operation'])
    df = (
        df
        .rename(columns={'type': 'operation'})
        .loc[:, ['id', 'ticker', 'amount', 'date', 'operation']]
        .sort_values('id')
        .pipe(reset_purchase_df_index)
    )
    return df


def add_user_purchase_data_to_db(passphrase, data):
    hash_ = hash_passphrase(passphrase)
    db = get_firestore_client()
    data['hash'] = hash_
    data['type'] = data.pop('operation')

    purchase_added_last = (
        db.collection('purchases')
        .order_by('id', direction=firestore.Query.DESCENDING)
        .limit(1)
        .get()
    )

    if purchase_added_last:
        new_id = purchase_added_last[0].to_dict()['id'] + 1
    else:
        new_id = 1
    data['id'] = new_id

    write_dict_to_firestore(db, 'purchases', data)


def delete_user_purchase_data(passphrase, id_):
    hash_ = hash_passphrase(passphrase)
    db = get_firestore_client()
    docs = db.collection(collection_name).where('hash', '==', hash_).where('id', '==', id_).stream()
    for doc in docs:
        doc.delete()
