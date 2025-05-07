from collections import Counter
from sklearn.cluster import KMeans
import numpy as np
import pandas as pd

def estimate_periodicity(diffs, method='mode', n_clusters=2):
    diffs = np.array(diffs).reshape(-1, 1)

    if method == 'mode':
        return Counter(diffs.flatten()).most_common(1)[0][0]
    elif method == 'median':
        return int(np.median(diffs))
    elif method == 'mean':
        return int(np.round(np.mean(diffs)))
    elif method == 'kmeans':
        model = KMeans(n_clusters=n_clusters, n_init='auto', random_state=42)
        model.fit(diffs)
        # Use the largest cluster center (assumes longer intervals are dominant)
        centers = sorted(model.cluster_centers_.flatten())
        return int(round(centers[-1]))
    else:
        raise ValueError("Method must be one of: 'mode', 'median', 'mean', 'kmeans'")

def assess_update_periodicity(df, diff_col, method='mode', tolerance_days=1, n_clusters=2):
    diffs = df[diff_col].dropna()

    period = estimate_periodicity(diffs, method=method, n_clusters=n_clusters)

    lower_bound = period - tolerance_days
    upper_bound = period + tolerance_days

    df['within_expected_range'] = df[diff_col].between(lower_bound, upper_bound)
    compliance_rate = df['within_expected_range'].mean()

    return df, {
        'estimation_method': method,
        'estimated_period': period,
        'expected_range': (lower_bound, upper_bound),
        'compliance_rate': compliance_rate
    }
