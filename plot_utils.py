import pandas as pd
import plotly.express as px


def get_asset_pie_plot_fig(s: pd.Series, name):
    fig = px.pie(
        s,
        values=s.values,
        names=s.index,
        title=f"{name} worth {int(s.sum()):,} PLN",
    )
    fig.update_traces(
        textinfo='percent+label',
        textfont_size=13,
    )
    fig.update_layout(showlegend=False)
    return fig


def generate_historical_net_worth_stacked_area_plot(df):
    ax = (
        df
        .plot
        .area(
            figsize=(9, 9),
            legend='reverse',
            title='Historical net worth',
            linewidth=0
        )
    )
    ax.ticklabel_format(style='plain', axis='y')
    ax.set_ylabel('PLN')
    ax.autoscale(tight=True)
    return ax.get_figure()
