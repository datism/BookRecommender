import json
import importlib
import argparse
from pyspark.sql import SparkSession

def _parse_arguments():
    # Parse arguments provided by spark-submit command
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True)
    parser.add_argument("--user", default=-1, required=False)
    parser.add_argument("--book", default=-1, required=False)
    return parser.parse_args()

def main():
    # Main function executed bt spark-submit commmand
    args = _parse_arguments()

    with open("app/src/config.json", "r") as config_file:
        config = json.load(config_file)
    
    spark = SparkSession.builder.appName(config.get("APP_NAME")).getOrCreate()

    job_module = importlib.import_module(f"jobs.{args.job}")
    config.update({'user': args.user})
    config.update({'book': args.book})

    job_module.run_job(spark, config)
    
if __name__ == "__main__":
    main()