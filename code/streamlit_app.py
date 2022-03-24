import os
import random

import pandas as pd
import streamlit as st
import yfinance as yf
from english_words import english_words_lower_alpha_set
from streamlit_cookies_manager import EncryptedCookieManager

import data_utils
import database as db
import plot_utils


def handle_purchase_form(
        purchase_df,
        user_ticker,
        purchase_form_error,
        user_amount,
        user_date,
        user_operation,
        passphrase
):
    user_ticker = user_ticker.upper()
    # validate ticker
    if not user_ticker:
        purchase_form_error.error(f'missing ticker name')
        return
    if not db.is_ticker_in_db(user_ticker):
        ticker_info = yf.Ticker(user_ticker).info
        if ticker_info == {'regularMarketPrice': None, 'preMarketPrice': None, 'logo_url': ''}:
            purchase_form_error.error(f'ticker "{user_ticker}" does not exist')
            return
        elif ticker_info['quoteType'] == 'CURRENCY':
            purchase_form_error.error(f'ticker "{user_ticker}" is a currency')
            return
        else:
            db.add_ticker_to_db(user_ticker, ticker_info['currency'], ticker_info['quoteType'])

    # validate date
    if pd.Timestamp(user_date) > pd.Timestamp.now():
        purchase_form_error.error(f'date "{user_date}" is in the future')
        return

    # validate amount and operation
    if user_amount <= 0:
        purchase_form_error.error(f"amount {user_amount} is too low")
        return

    if user_operation == 'sale':
        if user_ticker not in purchase_df.ticker.values:
            purchase_form_error.error(f"can't sell before buying")
            return

        date = pd.Timestamp(user_date)
        bought_till_date = (
            purchase_df
            .query(f'ticker == "{user_ticker}"')
            .assign(date=lambda x: pd.to_datetime(x.date))
            .query(f'date <= "{str(date)}"')
            .query(f'operation == "purchase"')
            .amount
            .sum()
        )
        sold_till_date = (
            purchase_df
            .query(f'ticker == "{user_ticker}"')
            .assign(date=lambda x: pd.to_datetime(x.date))
            .query(f'date <= "{str(date)}"')
            .query(f'operation == "sale"')
            .amount
            .sum()
        )
        if user_amount > (bought_till_date - sold_till_date):
            purchase_form_error.error(
                f"can't sell more ({user_amount}) than purchased ({bought_till_date - sold_till_date})"
            )
            return

    # add purchase
    data = {
        'ticker': user_ticker.upper(),
        'amount': user_amount,
        'date': str(user_date),
        'operation': user_operation,
    }
    db.add_user_purchase_data_to_db(passphrase, data)


if __name__ == '__main__':

    ## COOKIES ##

    cookies = EncryptedCookieManager(
        prefix="mkusm/invest_dashboard/",
        password=os.environ.get("COOKIES_PASSWORD", "dev_env_password"),
    )
    if not cookies.ready():
        # Wait for the component to load and send us current cookies.
        st.stop()

    user_passphrase = cookies.get('passphrase')
    if user_passphrase is None:
        passphrase = []
        for i in range(6):
            passphrase.append(random.choice(list(english_words_lower_alpha_set)))
        passphrase = ' '.join(passphrase)
        cookies['passphrase'] = passphrase
        cookies.save()


    ## SIDEBAR ##

    with st.sidebar.form('purchase-form'):
        st.caption(
            'Choose a ticker with yahoo finance format like AAPL/GOOG for equities,'
            ' VWCE.DE/CSPX.L for ETFs, BTC-USD/DOGE-USD for cryptocurrencies.'
        )
        user_ticker = st.text_input('ticker name')
        user_amount = st.number_input('amount', min_value=0.0, step=0.0001, format="%.4f")
        user_date = st.date_input(
            'date',
            value=(pd.Timestamp.now() - pd.Timedelta(1, unit='W')).date(),
            min_value=(pd.Timestamp.now() - pd.Timedelta(50 * 365, unit='days')).date(),
            max_value=pd.Timestamp.now().date(),
        )
        user_operation = st.radio('operation type', ('purchase', 'sale'))
        purchase_form_error = st.empty()
        submit_add = st.form_submit_button('add operation')

    purchase_df = db.get_user_purchase_data_from_db(cookies['passphrase'])
    if submit_add:
        handle_purchase_form(
            purchase_df,
            user_ticker,
            purchase_form_error,
            user_amount,
            user_date,
            user_operation,
            cookies['passphrase']
        )
        purchase_df = db.get_user_purchase_data_from_db(cookies['passphrase'])

    st.sidebar.write('operations')
    user_purchase_table = st.sidebar.empty()

    with st.sidebar.form('delete-form'):
        purchase_id = st.number_input(
            'operation id',
            min_value=purchase_df.index[0] if purchase_df.shape[0] > 0 else 0,
            max_value=purchase_df.index[-1] if purchase_df.shape[0] > 0 else 0,
        )
        delete_form_error = st.empty()
        submit_delete = st.form_submit_button('delete operation')

    if submit_delete:
        if purchase_id not in purchase_df.index:
            delete_form_error.error(f'operation with id {purchase_id} does not exist')
        else:
            id_ = purchase_df.loc[purchase_id, 'id']
            purchase_df = purchase_df.drop(purchase_id).pipe(data_utils.reset_purchase_df_index)
            db.delete_user_purchase_data(cookies['passphrase'], id_)
            purchase_df = db.get_user_purchase_data_from_db(cookies['passphrase'])
            st.experimental_rerun()

    user_purchase_table.write(purchase_df.drop(columns=['id']), height=2000, width=2000)

    with st.sidebar.expander('user passphrase identifier'):
        with st.form('passphrase-form'):
            st.write(f'Passphrase that identifies you is: "{cookies["passphrase"]}"')
            user_passphrase = st.text_input('Type your passphrase below to see your data on new device.')
            submit_passphrase = st.form_submit_button('submit passphrase')

        if submit_passphrase:
            cookies['passphrase'] = user_passphrase
            st.experimental_rerun()


    ## DASHBOARD ##

    # gather data
    if purchase_df.empty:
        if 'random_purchase_data' not in st.session_state:
            purchase_df = db.generate_random_purchase_data()
            st.session_state['random_purchase_data'] = purchase_df
        else:
            purchase_df = st.session_state['random_purchase_data']
        st.info('Data below is randomly generated. Add your own data in the sidebar.')
    ticker_info_df = db.create_ticker_df_with_currency_and_type(purchase_df.ticker.unique())

    hide_streamlit_style = """
        <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden; }
        footer:before {
            content: 'source available at github.com/mkusm/invest-dashboard';
            visibility: visible;
            display: block;
            position: relative;
        }
        </style>
    """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)

    with st.spinner('Downloading historical stock splits...'):
        purchase_df = purchase_df.pipe(data_utils.correct_asset_amount_affected_by_split, ticker_info_df.type)
    assets_df = data_utils.calculate_current_assets_from_purchases_and_sales(purchase_df, ticker_info_df)
    earliest_date = pd.to_datetime(purchase_df.loc[:, 'date']).min().strftime('%Y-%m-%d')
    assets_names_with_currencies = pd.concat(
        [
            assets_df.index.to_series(),
            pd.Series(assets_df.currency.unique()) + 'USD=X',
            pd.Series(['PLNUSD=X'])
        ]
    ).reset_index(drop=True).sort_values()
    five_min_unique_date = pd.Timestamp.now().replace(minute=pd.Timestamp.now().minute // 5, second=0, microsecond=0)
    with st.spinner('Downloading historical asset prices...'):
        historical_prices = data_utils.get_historical_prices(
            assets_names_with_currencies.to_list(),
            start=earliest_date,
            # cache_date makes sure historical data isn't redownloaded more than once every five minutes
            cache_date=five_min_unique_date,
        )
    if set(historical_prices.columns) != set(assets_names_with_currencies.to_list()):
        # rare streamlit cache bug
        st.experimental_rerun()
    assets_df = assets_df.pipe(data_utils.add_latest_asset_prices, historical_prices)
    total_pie_figure = plot_utils.get_asset_pie_plot_fig(assets_df.groupby('type').sum().total_pln, 'Total net')
    type_pie_figures = [
        plot_utils.get_asset_pie_plot_fig(assets_df.query(f'type == "{type_}"').total_pln, type_.capitalize())
        for type_ in assets_df.type.unique()
    ]

    # show assets df
    with st.expander('Current assets table (click to show/hide)'):
        st.dataframe(
            assets_df.drop(columns=['currency_rate']).query('amount != 0').style.format(precision=2),
            height=1000
        )

    # plot pie plots
    st.plotly_chart(total_pie_figure, use_container_width=True)
    with st.expander('Asset category pie charts (click to show/hide)'):
        for fig in type_pie_figures:
            st.plotly_chart(fig, use_container_width=True)

    # radio and slider widgets
    frequency = st.radio("Historical net worth aggregation", ("Day","Week","Month"))
    frequency = {'Day': 'D', 'Week': 'W', 'Month': 'M'}[frequency]
    max_value = (pd.Timestamp.now().to_period('M') - pd.Timestamp(earliest_date).to_period('M')).n + 2
    months_n = st.slider('Historical net worth last n months', min_value=1, max_value=max_value, value=max_value)

    # plot historical area plot using above widgets
    historical_value_in_pln = data_utils.calculate_historical_value_in_pln(
        historical_prices, purchase_df, assets_df, months_n, frequency
    )
    fig = plot_utils.generate_historical_net_worth_stacked_area_plot(historical_value_in_pln.ffill())
    st.pyplot(fig)
