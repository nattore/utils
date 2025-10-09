import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

def plot_proportion_bar_chart(df, date_col, proportion_col):
    """
    Generates and displays a bar chart for a single series of proportions over time.

    Args:
        df (pd.DataFrame): The input DataFrame.
        date_col (str): The name of the column containing the dates.
        proportion_col (str): The name of the column containing the proportion values.
    """
    # --- 1. Data Preparation ---
    # Create a copy to avoid modifying the original DataFrame
    plot_df = df.copy()

    # Ensure the date column is in datetime format
    plot_df[date_col] = pd.to_datetime(plot_df[date_col])

    # Set the date column as the index for plotting
    plot_df.set_index(date_col, inplace=True)

    # --- 2. Plotting ---
    # Create the figure and axes objects
    fig, ax = plt.subplots(figsize=(14, 8))

    # Create the bar chart from the specified proportion column
    plot_df[proportion_col].plot(kind='bar', ax=ax, width=0.7, legend=False)

    # --- 3. Adding Labels to Each Bar ---
    # Add data labels on top of each bar
    # ax.containers[0] refers to the single set of bars in the plot
    ax.bar_label(
        ax.containers[0],
        fmt='{:.2%}', # Format the label as a percentage
        padding=3,    # Add some space above the bar
        color='dimgray',
        weight='bold'
    )

    # --- 4. Formatting and Styling ---
    # Format the y-axis to show percentages
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

    # Adjust y-axis limits to give space for the labels on top
    # We set the top limit to be 10% higher than the max proportion
    max_proportion = plot_df[proportion_col].max()
    ax.set_ylim(0, max_proportion * 1.1)

    # Set titles and labels
    ax.set_title(f'{proportion_col.replace("_", " ").title()} Over Time', fontsize=16, pad=20)
    ax.set_ylabel('Proportion', fontsize=12)
    ax.set_xlabel(date_col.replace('_', ' ').capitalize(), fontsize=12)

    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')

    # Use a clean layout
    plt.tight_layout()

    # Display the plot
    plt.show()

