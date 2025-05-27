dtype_map = {}

for col in numeric_cols:
    col_min = min_max_df[f"{col}_min"].iloc[0]
    col_max = min_max_df[f"{col}_max"].iloc[0]
    original_dtype = original_types[col]

    # Skip if column has nulls (min/max will be NaN)
    if pd.isna(col_min) or pd.isna(col_max):
        continue

    # Determine dtype for integers
    if "int" in original_dtype:
        if col_min >= 0:
            # Unsigned integers
            for dtype in [np.uint8, np.uint16, np.uint32]:
                if col_max <= np.iinfo(dtype).max:
                    dtype_map[col] = dtype.name
                    break
        else:
            # Signed integers
            for dtype in [np.int8, np.int16, np.int32]:
                if col_min >= np.iinfo(dtype).min and col_max <= np.iinfo(dtype).max:
                    dtype_map[col] = dtype.name
                    break

    # Determine dtype for floats
    elif "double" in original_dtype or "float" in original_dtype:
        dtype_map[col] = "float32"  # Downcast to float32 by default
