import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.patheffects as path_effects
import numpy as np

def apply_custom_theme():
    sns.set_theme(style="white")
    sns.set_context("notebook", font_scale=1.2)

    custom_params = {
        "axes.edgecolor": "0.3",
        "axes.linewidth": 0.8,
        "xtick.color": "0.3",
        "ytick.color": "0.3",
        "axes.titlesize": 13,
        "axes.labelsize": 12,
        "legend.fontsize": 10,
        "legend.title_fontsize": 11,
        "figure.dpi": 100,
        "figure.facecolor": "white",
    }
    sns.set_style("white", rc=custom_params)

def plot_single_score_type(df, score_type):
    apply_custom_theme()

    sub_df = df[df['score_type'] == score_type].copy()
    sub_df['interval'] = pd.Categorical(sub_df['interval'], categories=sorted(sub_df['interval'].unique()), ordered=True)

    # Prepare labels
    anomes_sorted = sorted(sub_df['anomes'].unique())
    anomes_str_sorted = [f"{str(ano)[:4]}-{str(ano)[4:]}" for ano in anomes_sorted]
    x = list(range(len(anomes_sorted)))  # Numeric positions

    fig, ax = plt.subplots(figsize=(10, 6))

    interval_colors = sns.color_palette("coolwarm", n_colors=sub_df['interval'].nunique())
    bottom = pd.Series(0, index=anomes_sorted, dtype=float)

    for interval, color in zip(sub_df['interval'].cat.categories, interval_colors):
        layer = sub_df[sub_df['interval'] == interval].set_index('anomes').reindex(anomes_sorted)
        fractions = layer['fraction'].fillna(0).values

        bars = ax.bar(x, fractions, bottom=bottom.values, label=f'{interval}', color=color)

        for bar, value in zip(bars, fractions):
            if value > 0.01:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_y() + bar.get_height() / 2,
                    f"{value:.2f}",
                    ha='center', va='center',
                    fontsize=10,
                    color='black',
                    path_effects=[
                        path_effects.Stroke(linewidth=2.5, foreground='white'),
                        path_effects.Normal()
                    ]
                )

        bottom += fractions

    ax.set_title(f"Score Type: {score_type}", fontsize=16)
    ax.set_ylabel("")
    ax.set_ylim(0, 1)
    ax.set_facecolor("white")

    # Properly aligned x-axis labels
    ax.set_xticks(x)
    ax.set_xticklabels(anomes_str_sorted, rotation=45, ha='right')

    # Remove y-axis ticks/labels for clean look
    ax.set_yticks([])
    ax.set_yticklabels([])

    sns.despine(ax=ax, left=True, bottom=False)
    ax.grid(False)

    ax.legend(
        title='Score Range',
        loc='center left',
        bbox_to_anchor=(1.0, 0.5),
        # ncol=sub_df['interval'].nunique(),
        frameon=True,
        handletextpad=0.5,
        # columnspacing=1.2,
        borderpad=0.5,
    )

    # plt.tight_layout()
    plt.show()


def plot_all_score_types(df):
    for score_type in df['score_type'].unique():
        plot_single_score_type(df, score_type)


anomes_list = [202401 + i for i in range(10)]
score_types = ['prob1', 'prob2', 'prob3']
intervals = ['0.0 - 0.2', '0.2 - 0.4', '0.4 - 0.6', '0.6 - 0.8', '0.8 - 1.0']

rows = []
rng = np.random.default_rng(42)

for anomes in anomes_list:
    for score_type in score_types:
        # Generate 5 random fractions that sum to 1
        fractions = rng.dirichlet([1]*5).tolist()
        for interval, fraction in zip(intervals, fractions):
            rows.append([anomes, score_type, interval, fraction])

df10 = pd.DataFrame(rows, columns=['anomes', 'score_type', 'interval', 'fraction'])


plot_all_score_types(df10)
