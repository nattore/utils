import polars as pl
from functools import reduce
import operator
from datetime import date

# 🗺️ A map of operation strings to Polars expression generators (dependency)
OPERATION_MAP = {
    "==": lambda col, val: col == val,
    "!=": lambda col, val: col != val,
    ">": lambda col, val: col > val,
    ">=": lambda col, val: col >= val,
    "<": lambda col, val: col < val,
    "<=": lambda col, val: col <= val,
    "isin": lambda col, val: col.is_in(val),
    "isnotin": lambda col, val: ~col.is_in(val),
    "contains": lambda col, val: col.str.contains(val),
    "startswith": lambda col, val: col.str.starts_with(val),
    "endswith": lambda col, val: col.str.ends_with(val),
    "is_null": lambda col, val: col.is_null(),
    "is_not_null": lambda col, val: col.is_not_null(),
}


def build_polars_expression(conditions: list[list]) -> pl.Expr:
    """
    Generates a compound Polars expression from a list of conditions.

    Args:
        conditions: A list of 3-element lists, where each inner list
                    is [column_name, operation_string, value].

    Returns:
        A single Polars expression combining all conditions with AND logic.
    """
    if not conditions:
        # Return a neutral expression that selects all rows
        return pl.lit(True)

    # 1. Generate a list of individual Polars expressions
    expressions = [OPERATION_MAP[op](pl.col(col), val) for col, op, val in conditions]

    # 2. Combine all expressions with a logical AND
    combined_expression = reduce(operator.and_, expressions)

    return combined_expression


def example1():
    # --- Example Usage ---

    # 1. Create a sample DataFrame of employees
    df = pl.DataFrame(
        {
            "employee_id": [101, 102, 103, 104, 105],
            "department": [
                "Sales",
                "Engineering",
                "Engineering",
                "Sales",
                "Engineering",
            ],
            "salary": [85000, 120000, 95000, 92000, 150000],
            "start_date": [
                date(2021, 6, 15),
                date(2020, 3, 10),
                date(2022, 2, 20),
                date(2023, 8, 1),
                date(2022, 5, 12),
            ],
        }
    )

    # 2. Define a list of filtering conditions
    # We want: department is 'Engineering' AND salary > 90000 AND start_date >= 2022-01-01
    filters_to_apply = [
        ["department", "==", "Engineering"],
        ["salary", ">", 90000],
        ["start_date", ">=", date(2022, 1, 1)],
    ]

    # 3. Build the combined expression from the list
    final_expression = build_polars_expression(filters_to_apply)

    # 4. Apply the filter and get the result
    result = df.filter(final_expression)

    # --- Display Results ---
    print("🏢 Original DataFrame:")
    print(df)
    print("\n📋 Filters Applied:")
    print(filters_to_apply)
    print("\n✅ Filtered Result:")
    print(result)


def example2():
    # --- LazyFrame Example ---

    # 1. Define the URL for the public dataset
    # This file contains NYC Yellow Taxi trip records for January 2024.
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

    # 2. Lazily scan the Parquet file from the URL
    # This does NOT download the data yet. It only inspects the file's metadata.
    print(f"🚕 Scanning Parquet file from URL...\n")
    lf = pl.scan_parquet(url)

    # 3. Define a list of filtering conditions
    # We want trips paid by credit card, with more than 1 passenger,
    # and a trip distance of at least 10 miles.
    filters_to_apply = [
        ["payment_type", "==", 1], # 1 = Credit card in this dataset
        ["passenger_count", ">", 1],
        ["trip_distance", ">=", 10.0]
    ]

    # 4. Build the expression and apply it to the LazyFrame
    final_expression = build_polars_expression(filters_to_apply)
    result_lf = lf.filter(final_expression)

    # 5. Show the optimized query plan
    # Notice how the filters are "pushed down" into the PARQUET SCAN step.
    # This means Polars will filter the data while reading it, which is very fast.
    print("📊 Optimized Query Plan:")
    print(result_lf.describe())

    # 6. Execute the query and collect the results
    # NOW the data is downloaded and processed according to the plan.
    print("\n💨 Collecting results...\n")
    result_df = result_lf.collect()

    # --- Display Results ---
    print("✅ Final Filtered DataFrame:")
    print(result_df)


if __name__ == "__main__":
    # example1()
    example2()
