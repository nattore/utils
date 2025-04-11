def plot_cdf_multiple_anomes(
    df,
    anomes_list,
    atraso_range=None,
    highlight_targets=None,
    max_points=1_000_000,
    max_percentile=None,
    save_to_file=False,
    file_prefix="cdf_plot",
    df2=None,
    label1="Dataset 1",
    label2="Dataset 2"
):
    for anomes in anomes_list:
        file_name = f"{file_prefix}_{anomes}.png" if save_to_file else None
        print(f"Generating plot for anomes = {anomes}")
        plot_target_cdf(
            df=df,
            anomes=anomes,
            atraso_range=atraso_range,
            highlight_targets=highlight_targets,
            max_points=max_points,
            max_percentile=max_percentile,
            save_to_file=save_to_file,
            file_name=file_name,
            df2=df2,
            label1=label1,
            label2=label2
        )
