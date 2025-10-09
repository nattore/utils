import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

def plot_stacked_proportions(df, date_col):
    """
    Generates and displays a stacked bar chart with proportion labels.

    The function assumes that for each date, the sum of the values in the
    other numeric columns equals 1 (i.e., they are proportions).

    Args:
        df (pd.DataFrame): The input DataFrame. It should contain a date
                           column and one or more numeric columns
                           representing proportions.
        date_col (str): The name of the column containing the dates.
    """
    # --- 1. Data Preparation ---
    # Create a copy to avoid modifying the original DataFrame
    plot_df = df.copy()

    # Ensure the date column is in datetime format for proper sorting and plotting
    plot_df[date_col] = pd.to_datetime(plot_df[date_col])

    # Set the date column as the index, which is the standard for pandas plotting
    plot_df.set_index(date_col, inplace=True)
    
    # Identify proportion columns (all columns that are not the index)
    proportion_cols = plot_df.columns

    # --- 2. Plotting ---
    # Create the figure and axes objects
    fig, ax = plt.subplots(figsize=(14, 8))

    # Create the stacked bar chart
    plot_df[proportion_cols].plot(
        kind='bar',
        stacked=True,
        ax=ax,
        width=0.8,
        edgecolor="white" # Add a white edge for better segment distinction
    )

    # --- 3. Adding Labels to Each Segment ---
    # The core of the request: iterate through the bar containers
    for container in ax.containers:
        # Create custom labels, hiding them for very small segments
        labels = []
        for v in container.datavalues:
            # Only add a label if the proportion is > 2%
            if v > 0.02:
                # Format as a float with two decimal places
                labels.append(f'{v:.2f}')
            else:
                labels.append('') # Keep the label empty for small segments

        ax.bar_label(
            container,
            labels=labels,
            label_type='center',
            color='white',
            weight='bold',
            fontsize=10
        )

    # --- 4. Formatting and Styling ---
    # Format the y-axis to show percentages
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_ylim(0, 1)

    # Set titles and labels for clarity
    ax.set_title('Stacked Proportions Over Time', fontsize=16, pad=20)
    ax.set_ylabel('Proportion', fontsize=12)
    ax.set_xlabel(date_col.replace('_', ' ').capitalize(), fontsize=12)

    # Improve the legend
    ax.legend(title='Category', bbox_to_anchor=(1.02, 1), loc='upper left')

    # Rotate x-axis labels for better readability if they are long
    plt.xticks(rotation=45, ha='right')

    # Ensure everything fits without overlapping
    plt.tight_layout()

    # Display the plot
    plt.show()

