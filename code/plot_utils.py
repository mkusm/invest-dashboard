import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt


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
            title='Historical net worth (PLN)',
            linewidth=0
        )
    )
    plt.draw()
    ax.autoscale(tight=True)
    ax.set_xlabel('')
    ax.ticklabel_format(style='plain', axis='y')

    labels = ax.get_yticklabels()
    for text in labels:
        if text._y == 0:
            text.set_text('')
        elif text._y >= 1000 and text._y < 1000000:
            text.set_text(f'{text._y/1000:g}k')
        elif text._y >= 1000000:
            text.set_text(f'{text._y/1000000:g}M')
    ax.set_yticklabels(labels)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        handles[::-1],
        labels[::-1],
        loc='upper center',
        bbox_to_anchor=(0.5, -0.04),
        ncol=5,
        fancybox=True,
    )

    return ax.get_figure()
