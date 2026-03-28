import great_expectations as gx
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", "5433")

context = gx.get_context()

datasource_config = {
    "name": "f1_postgres",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "postgresql+psycopg2://f1admin:f1analytics2025@postgres:5432/f1_warehouse"
    },
    "data_connectors": {
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "include_schema_name": True,
        }
    },
}


context.add_or_update_datasource(**datasource_config)
print(f"Datasource created with host={host} port={port}")
