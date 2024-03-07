import argparse
from data_processing import (
    read_csv_data,
    clean_data,
    validate_data,
    transform_data,
    publish_data
)

def main(csv_file_path: str):
    """
    Orchestrates the data pipeline process from reading CSV data to publishing it to the database.

    :param csv_file_path: Path to the CSV file to be processed.
    """
    # Step 1: Read CSV data
    raw_data = read_csv_data(csv_file_path)
    print("CSV data read successfully.")

    # Step 2: Clean data
    cleaned_data = clean_data(raw_data)
    print("Data cleaned.")

    # Step 3: Validate data
    validated_data = validate_data(cleaned_data)
    print("Data validated.")

    # Step 4: Transform data
    product_dim, retailer_dim, date_dim, sales_fact = transform_data(validated_data)
    print("Data transformed.")

    # Step 5: Publish data
    publish_data(product_dim, retailer_dim, date_dim, sales_fact)
    print("Data published to the database successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Pipeline Orchestrator CLI Tool")
    parser.add_argument(
        "csv_file_path",
        type=str,
        help="Path to the CSV file to be processed."
    )

    args = parser.parse_args()

    main(args.csv_file_path)
