from typing import List, Dict, Any, Generator
import os
import psycopg2
from contextlib import contextmanager
import logging
import csv
from datetime import datetime


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@contextmanager
def database_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    connection = psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "sales"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "6543"),
    )
    try:
        yield connection
    finally:
        connection.close()

def read_csv_data(file_path: str) -> List[Dict[str, Any]]:
    """
    Reads data from a CSV file and returns a list of dictionaries.

    Parameters:
        file_path (str): The path to the CSV file.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary represents a row from the CSV file.
            The keys of the dictionaries are the column names, and the values are the corresponding row values.
    """
    sales_data = []
    with open(file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            sales_data.append(row)
    return sales_data

def clean_location(value: str) -> str:
    if value == '':
        return None
    else:
        return value
    
def clean_price(value: str) -> float:
    """
    Clean price values, removing currency symbols and converting to float.
    """
    # Remove non-numeric characters except the decimal point
    cleaned_value = ''.join(c for c in value if c.isdigit() or c == '.')
    return float(cleaned_value) if cleaned_value else 0.0

def clean_date(value: str) -> str:
    """
    Clean date values by formatting them to a standard format (YYYY-MM-DD).

    Args:
        value (str): The date value to be cleaned.

    Returns:
        str: The cleaned date value in the format 'YYYY-MM-DD'.
    """
    # Split the date into its components
    if  '/' in value:
        value = value.replace('/', '-')
        return value
    else:
        return value  # Return the original value if it's not in the expected format

def clean_quantity(value: str) -> int:
    try:
        value = int(value)
        if value >= 0:
            return value
        else:
            return 0
    except ValueError:
        return 0

def clean_value(key: str, value: str) -> Any:
    """
    Clean data based on key, including handling of date formats.
    """
    if key == "Location":
        return clean_location(value)
    if key == "Price":
        return clean_price(value)
    elif key == "Date":
        return clean_date(value)
    elif key == "Quantity":
       return clean_quantity(value)
    else:
        return value


def clean_data(data: List[Dict[str, str]], id_fields: List[str] = ['ProductID', 'SaleID', 'RetailerID']) -> List[Dict[str, Any]]:
    """
    Clean data by applying specific cleaning logic based on the key of each item.
    Log and exclude rows where any of the specified ID fields are missing or not integers.

    Parameters:
    - data: The list of data rows to clean.
    - id_fields: The list of ID fields to validate for each row.

    Returns:
    - The cleaned data as a list of dictionaries.
    """
    cleaned_data: List[Dict[str, Any]] = []
    seen: set = set()
    final_cleaned_data : List[Dict[str, Any]] = []

    for row in data:
        valid_row = True
        for field in id_fields:
            if field not in row or not row[field]:
                valid_row = False
                logging.warning(f"Row excluded: {row} - missing field: {field}")
                break
            if field in id_fields and not row[field].isdigit():
                valid_row = False
                logging.warning(f"Row excluded: {row} - invalid {field}: {row[field]}")
                break
        if valid_row:
            cleaned_data.append(row)

    for row in cleaned_data:
        cleaned_strip_row = {key: value.strip() for key, value in row.items()}
        cleaned_clean_value_row = {key: clean_value(key, value) for key, value in cleaned_strip_row.items()}
        identifier = tuple(sorted(cleaned_clean_value_row.items()))
        if identifier not in seen:
            seen.add(identifier)
            final_cleaned_data.append(cleaned_clean_value_row)
    return final_cleaned_data

def validate_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Validate the cleaned data and return a list of dictionaries representing valid rows.
    For this exercise, we assume that all data is valid.
    """
    validated_data = []

    for row in data:
        valid_row = True
        for field in ['ProductID', 'SaleID', 'RetailerID']:
            if field not in row or not row[field]:
                valid_row = False
                logging.warning(f"Row excluded: {row} - missing field: {field}")
                break
 
        if valid_row:
            validated_data.append(row)
    return validated_data


def transform_data(cleaned_data: list[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], list[Dict[str, Any]], list[Dict[str, Any]], list[Dict[str, Any]]]:
    """
    Transform cleaned data to prepare it for analysis or loading into a database.

    Parameters:
    - cleaned_data: The cleaned data as a list of dictionaries.

    Returns:
    - product_dim: Transformed product dimension data as a list of dictionaries.
    - retailer_dim: Transformed retailer dimension data as a list of dictionaries.
    - date_dim: Transformed date dimension data as a list of dictionaries.
    - sales_fact: Transformed sales fact data as a list of dictionaries.
    """
    product_dim = []
    retailer_dim = []
    date_dim = []
    sales_fact = []

    for row in cleaned_data:
        # Product Dimension
        if all([row['ProductID'] not in [p['ProductID'] for p in product_dim], row['ProductName']]):
            product_dim.append({
                'ProductID': int(row['ProductID']),
                'Name': row['ProductName'],
                'Brand': row['Brand'] if row['Brand'] else None,  # Allow null for Brand
                'Category': row['Category'] if row['Category'] else None  # Allow null for Category
            })

        # Retailer Dimension
        if all([row['RetailerID'] not in [r['RetailerID'] for r in retailer_dim], row['RetailerName']]):
            retailer_dim.append({
                'RetailerID': int(row['RetailerID']),
                'Name': row['RetailerName'],
                'Channel': row['Channel'] if row['Channel'] else None,
                'Location': row['Location'] if row['Location'] else None
            })

        # Date Dimension
        if row['Date'] not in [d['Date'] for d in date_dim]:
            date_parts = row['Date'].split('-')
            year = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])
            date_obj = datetime.strptime(row['Date'], '%Y-%m-%d')
            quarter = (month - 1) // 3 + 1
            day_of_week = date_obj.strftime('%A')
            week_of_year = date_obj.isocalendar()[1]

            # Only add the date if all fields are not null
            if all([year,month,day,quarter, day_of_week, week_of_year]):
                date_dim.append({
                    'Date': row['Date'],
                    'Day': day,
                    'Month': month,
                    'Year': year,
                    'Quarter': quarter,
                    'DayOfWeek': day_of_week,
                    'WeekOfYear': week_of_year
                })

        # Sales Fact
        if all([row['SaleID'] not in [r['SaleID'] for r in sales_fact]]):
            sales_fact.append({
                'SaleID': int(row['SaleID']),
                'ProductID': int(row['ProductID']),
                'RetailerID': int(row['RetailerID']),
                'Date': row['Date'],
                'Quantity': int(row['Quantity']),
                'Price': row['Price']
            })
    return product_dim, retailer_dim, date_dim, sales_fact

def publish_data(
    product_dim: List[Dict[str, Any]],
    retailer_dim: List[Dict[str, Any]],
    date_dim: List[Dict[str, Any]],
    sales_fact: List[Dict[str, Any]],
) -> None:
    """
    Publish data into PostgreSQL tables using merge (upsert) operation with update.

    Parameters:
    - product_dim (List[Dict[str, Any]]): The product dimension data to merge.
    - retailer_dim (List[Dict[str, Any]]): The retailer dimension data to merge.
    - date_dim (List[Dict[str, Any]]): The date dimension data to merge.
    - sales_fact (List[Dict[str, Any]]): The sales fact data to merge.
    """
    with database_connection() as conn:
        with conn.cursor() as cur:
            # Merge product_dim data
            for row in product_dim:
                columns = ', '.join(row.keys())
                values = ', '.join(['%s'] * len(row))
                update_values = ', '.join([f"{key} = EXCLUDED.{key}" for key in row.keys()])
                insert_query = f"INSERT INTO product_dim ({columns}) VALUES ({values}) ON CONFLICT (ProductID) DO UPDATE SET {update_values}"
                cur.execute(insert_query, list(row.values()))

            # Merge retailer_dim data
            for row in retailer_dim:
                columns = ', '.join(row.keys())
                values = ', '.join(['%s'] * len(row))
                update_values = ', '.join([f"{key} = EXCLUDED.{key}" for key in row.keys()])
                insert_query = f"INSERT INTO retailer_dim ({columns}) VALUES ({values}) ON CONFLICT (RetailerID) DO UPDATE SET {update_values}"
                cur.execute(insert_query, list(row.values()))

            # Merge date_dim data
            for row in date_dim:
                columns = ', '.join(row.keys())
                values = ', '.join(['%s'] * len(row))
                update_values = ', '.join([f"{key} = EXCLUDED.{key}" for key in row.keys()])
                insert_query = f"INSERT INTO date_dim ({columns}) VALUES ({values}) ON CONFLICT (Date) DO UPDATE SET {update_values}"
                cur.execute(insert_query, list(row.values()))

            # Merge sales_fact data
            for row in sales_fact:
                columns = ', '.join(row.keys())
                values = ', '.join(['%s'] * len(row))
                update_values = ', '.join([f"{key} = EXCLUDED.{key}" for key in row.keys()])
                insert_query = f"INSERT INTO sales_fact ({columns}) VALUES ({values}) ON CONFLICT (SaleID) DO UPDATE SET {update_values}"
                cur.execute(insert_query, list(row.values()))

            conn.commit()