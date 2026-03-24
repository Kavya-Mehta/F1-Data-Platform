import great_expectations as gx
from dotenv import load_dotenv

load_dotenv()

context = gx.get_context()

checkpoint_config = {
    "name": "f1_raw_checkpoint",
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": {
                "datasource_name": "f1_postgres",
                "data_connector_name": "default_inferred_data_connector_name",
                "data_asset_name": "raw.results",
            },
            "expectation_suite_name": "raw_results_quality",
        }
    ],
}

context.add_or_update_checkpoint(**checkpoint_config)
print("Checkpoint created successfully!")
