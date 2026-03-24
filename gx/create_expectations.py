import great_expectations as gx
from dotenv import load_dotenv

load_dotenv()

context = gx.get_context()

# Create expectation suite for raw results table
suite_name = "raw_results_quality"
context.add_or_update_expectation_suite(expectation_suite_name=suite_name)

validator = context.get_validator(
    datasource_name="f1_postgres",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="raw.results",
    expectation_suite_name=suite_name
)

# Row count expectations
validator.expect_table_row_count_to_be_between(min_value=400, max_value=600)

# Column existence expectations
validator.expect_column_to_exist("driver_abbr")
validator.expect_column_to_exist("round_number")
validator.expect_column_to_exist("finish_position")
validator.expect_column_to_exist("grid_position")
validator.expect_column_to_exist("points")
validator.expect_column_to_exist("is_dnf")

# Null checks
validator.expect_column_values_to_not_be_null("driver_abbr")
validator.expect_column_values_to_not_be_null("round_number")
validator.expect_column_values_to_not_be_null("finish_position")

# Value range checks
validator.expect_column_values_to_be_between(
    column="finish_position", min_value=1, max_value=30
)
validator.expect_column_values_to_be_between(
    column="round_number", min_value=1, max_value=24
)
validator.expect_column_values_to_be_between(
    column="points", min_value=0, max_value=25
)

# Unique driver per race check
validator.expect_compound_columns_to_be_unique(
    column_list=["round_number", "driver_abbr"]
)

validator.save_expectation_suite(discard_failed_expectations=False)
print("Expectation suite created successfully!")

