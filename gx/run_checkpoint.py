import great_expectations as gx
from dotenv import load_dotenv

load_dotenv()

context = gx.get_context()

results = context.run_checkpoint(checkpoint_name="f1_raw_checkpoint")

if results.success:
    print("✓ All expectations passed!")
else:
    print("✗ Some expectations failed!")
    for validation_result in results.run_results.values():
        for result in validation_result["validation_result"]["results"]:
            if not result["success"]:
                print(f"  FAILED: {result['expectation_config']['expectation_type']}")
                print(f"  Details: {result['result']}")