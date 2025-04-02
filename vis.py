import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

def plot_distributions_by_period(df1, df2, columns, periods, plot_type="hist", bins=30):
    """
    Plots distribution comparisons over multiple time periods.
    
    Parameters:
        df1, df2: DataFrames with the same structure, containing a 'year_month' column.
        columns: List of column names to compare.
        periods: List of time periods to include.
        plot_type: Type of plot ('hist', 'box', or 'violin').
        bins: Number of bins (only for histograms).
    """
    df1['Dataset'] = 'Dataset 1'
    df2['Dataset'] = 'Dataset 2'
    combined_df = pd.concat([df1, df2])

    # Define a consistent color palette
    custom_palette = {"Dataset 1": "royalblue", "Dataset 2": "darkorange"}

    for feature in columns:
        fig, axes = plt.subplots(1, len(periods), figsize=(5 * len(periods), 5), sharey=True)
        
        for i, period in enumerate(periods):
            period_data = combined_df[combined_df['year_month'] == period]

            if plot_type == "hist":
                sns.histplot(data=period_data, x=feature, hue="Dataset", bins=bins, kde=True, 
                             element="step", palette=custom_palette, ax=axes[i])
                axes[i].set_title(f'Hist: {feature} ({period})')

            elif plot_type == "box":
                sns.boxplot(data=period_data, x="Dataset", y=feature, hue="Dataset", 
                            palette=custom_palette, dodge=False, ax=axes[i])
                axes[i].set_title(f'Box: {feature} ({period})')

            elif plot_type == "violin":
                sns.violinplot(data=period_data, x="Dataset", y=feature, hue="Dataset", 
                               palette=custom_palette, dodge=False, ax=axes[i])
                axes[i].set_title(f'Violin: {feature} ({period})')

        plt.suptitle(f'Distribution Comparison for {feature} Over Time')
        plt.show()

# Example Usage:
np.random.seed(42)
periods = ["202401", "202402", "202403", "202404", "202405", "202406"]

df1 = pd.DataFrame({
    'year_month': np.random.choice(periods, 600),
    'A': np.random.normal(0, 1, 600),
    'B': np.random.normal(5, 2, 600),
    'C': np.random.normal(-3, 1, 600)
})

df2 = pd.DataFrame({
    'year_month': np.random.choice(periods, 600),
    'A': np.random.normal(0.5, 1, 600),
    'B': np.random.normal(5.5, 2, 600),
    'C': np.random.normal(-2.5, 1, 600)
})

# Call function to plot distributions (Choose one type: 'hist', 'box', or 'violin')
plot_distributions_by_period(df1, df2, ['A', 'B', 'C'], periods, plot_type="hist")
plot_distributions_by_period(df1, df2, ['A', 'B', 'C'], periods, plot_type="box")
plot_distributions_by_period(df1, df2, ['A', 'B', 'C'], periods, plot_type="violin")
