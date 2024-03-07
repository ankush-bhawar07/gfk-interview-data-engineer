import os
import pytest
import psycopg2
import contextlib
import logging
from io import StringIO
from src.data_processing import clean_data, validate_data, transform_data, read_csv_data


@pytest.fixture(autouse=True)
def setup_and_teardown_database():
    """Automatically manage database state before and after tests."""
    db_config = {
        "dbname": os.getenv('PGDATABASE', 'sales'),
        "user": os.getenv('PGUSER', 'postgres'),
        "password": os.getenv('PGPASSWORD', 'postgres'),
        "host": os.getenv('PGHOST', 'localhost'),
        "port": os.getenv('PGPORT', '6543'),
    }
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # Temporarily disable foreign key checks for truncation.
    cur.execute("SET session_replication_role = replica;")
    tables = ['Sales_Fact', 'Date_Dim', 'Retailer_Dim', 'Product_Dim']
    for table in tables:
        cur.execute(f"TRUNCATE TABLE {table} CASCADE;")
    cur.execute("SET session_replication_role = DEFAULT;")

    conn.commit()
    yield conn
    conn.close()


@contextlib.contextmanager
def capture_logging():
    """A context manager to capture logging output."""
    log_capture_string = StringIO()
    ch = logging.StreamHandler(log_capture_string)
    ch.setLevel(logging.INFO)
    logging.getLogger().addHandler(ch)
    try:
        yield log_capture_string
    finally:
        logging.getLogger().removeHandler(ch)


def test_read_csv_data():
    # GIVEN
    file_path = './generated_sales_data.csv'
    expected_columns = [
        'SaleID', 'ProductID', 'ProductName', 'Brand', 'Category',
        'RetailerID', 'RetailerName', 'Channel', 'Location',
        'Quantity', 'Price', 'Date'
    ]

    # WHEN
    data = read_csv_data(file_path)

    # THEN
    assert list(data[0].keys()) == expected_columns, "Unexpected columns in data."
    assert data, "CSV file read resulted in no data."


def test_clean_data():
    # GIVEN
    raw_data = [
        {
            'SaleID': '1', 'ProductID': '101', 'ProductName': 'widgetA',
            'Brand': 'BrandA', 'Category': 'CategoryA', 'RetailerID': '1',
            'RetailerName': 'RetailerA', 'Channel': 'Online', 'Location': '',
            'Quantity': '1', 'Price': '19.99', 'Date': '2023-01-01'
        },
        {
            'SaleID': '2', 'ProductID': '102', 'ProductName': 'widgetB',
            'Brand': 'BrandB', 'Category': 'CategoryB', 'RetailerID': '2',
            'RetailerName': 'RetailerB', 'Channel': 'Offline', 'Location': 'New York',
            'Quantity': '2', 'Price': '29.99', 'Date': '2023-01-02'
        }
    ]
    expected_data = [
        {
            'SaleID': '1', 'ProductID': '101', 'ProductName': 'widgetA',
            'Brand': 'BrandA', 'Category': 'CategoryA', 'RetailerID': '1',
            'RetailerName': 'RetailerA', 'Channel': 'Online', 'Location': None,
            'Quantity': 1, 'Price': 19.99, 'Date': '2023-01-01'
        },
        {
            'SaleID': '2', 'ProductID': '102', 'ProductName': 'widgetB',
            'Brand': 'BrandB', 'Category': 'CategoryB', 'RetailerID': '2',
            'RetailerName': 'RetailerB', 'Channel': 'Offline', 'Location': 'New York',
            'Quantity': 2, 'Price': 29.99, 'Date': '2023-01-02'
        }
    ]

    # WHEN
    cleaned_data = clean_data(raw_data)
    cleaned_data_sorted = sorted(cleaned_data, key=lambda x: x['SaleID'])
    expected_sorted = sorted(expected_data, key=lambda x: x['SaleID'])

    # THEN
    assert cleaned_data_sorted == expected_sorted, "Cleaned data does not match expected."

def test_clean_data_with_currency_code():
    # GIVEN
    raw_data = [
        {
            'SaleID': '3', 'ProductID': '103', 'ProductName': 'widgetC',
            'Brand': 'BrandC', 'Category': 'CategoryC', 'RetailerID': '3',
            'RetailerName': 'RetailerC', 'Channel': 'In-store', 'Location': 'CityC',
            'Quantity': '2', 'Price': '150USD', 'Date': '2023-01-03'
        },
        {
            'SaleID': '4', 'ProductID': '104', 'ProductName': 'widgetD',
            'Brand': 'BrandD', 'Category': 'CategoryD', 'RetailerID': '4',
            'RetailerName': 'RetailerD', 'Channel': 'In-store', 'Location': 'CityD',
            'Quantity': '1', 'Price': '250EUR', 'Date': '2023-01-04'
        }
    ]
    expected_data = [
        {
            'SaleID': '3', 'ProductID': '103', 'ProductName': 'widgetC',
            'Brand': 'BrandC', 'Category': 'CategoryC', 'RetailerID': '3',
            'RetailerName': 'RetailerC', 'Channel': 'In-store', 'Location': 'CityC',
            'Quantity': 2, 'Price': 150.0, 'Date': '2023-01-03'
        },
        {
            'SaleID': '4', 'ProductID': '104', 'ProductName': 'widgetD',
            'Brand': 'BrandD', 'Category': 'CategoryD', 'RetailerID': '4',
            'RetailerName': 'RetailerD', 'Channel': 'In-store', 'Location': 'CityD',
            'Quantity': 1, 'Price': 250.0, 'Date': '2023-01-04'
        }
    ]

    # WHEN
    cleaned_data = clean_data(raw_data)
    cleaned_data_sorted = sorted(cleaned_data, key=lambda x: x['SaleID'])

    # THEN
    assert cleaned_data_sorted == expected_data, "Cleaned data with currency code does not match expected."


def test_validate_data():
    # GIVEN
    data = [
        {
            'SaleID': '1', 'ProductID': '101', 'ProductName': 'WidgetA',
            'Brand': 'BrandA', 'Category': 'CategoryA', 'RetailerID': '1',
            'RetailerName': 'RetailerA', 'Channel': 'Online', 'Location': 'CityA',
            'Quantity': '5', 'Price': '20.00', 'Date': '2023-01-01'
        },
        {
            'SaleID': '2', 'ProductID': '', 'ProductName': 'WidgetB',
            'Brand': 'BrandB', 'Category': 'CategoryB', 'RetailerID': '2',
            'RetailerName': 'RetailerB', 'Channel': 'Offline', 'Location': 'CityB',
            'Quantity': '0', 'Price': '-10.00', 'Date': 'not-a-date'
        }
    ]
    expected_validated_data = [
        {
            'SaleID': '1', 'ProductID': '101', 'ProductName': 'WidgetA',
            'Brand': 'BrandA', 'Category': 'CategoryA', 'RetailerID': '1',
            'RetailerName': 'RetailerA', 'Channel': 'Online', 'Location': 'CityA',
            'Quantity': '5', 'Price': '20.00', 'Date': '2023-01-01'
        }
    ]

    # WHEN
    validated_data = validate_data(data)

    # THEN
    assert validated_data == expected_validated_data, "Validation did not yield expected data."


def test_transform_data():
    # GIVEN
    raw_data = [
        {
            'SaleID': '1', 'ProductID': '101', 'ProductName': 'WidgetA',
            'Brand': 'BrandA', 'Category': 'CategoryA', 'RetailerID': '1', 'RetailerName': 'RetailerA',
            'Channel': 'Online', 'Location': 'CityA', 'Quantity': '5',
            'Price': '20.00', 'Date': '2023-01-01'
        },
        {
            'SaleID': '2', 'ProductID': '102', 'ProductName': 'WidgetB',
            'Brand': 'BrandB', 'Category': 'CategoryA', 'RetailerID': '2', 'RetailerName': 'RetailerB',
            'Channel': 'Offline', 'Location': 'CityB', 'Quantity': '3',
            'Price': '25.00', 'Date': '2023-01-02'
        },
    ]

    # WHEN
    product_dim, retailer_dim, date_dim, sales_fact = transform_data(raw_data)

    # THEN
    # Product dimension checks
    assert len(product_dim) == 2, "Unexpected number of unique products."
    assert all('ProductID' in product for product in product_dim), "Missing ProductID in product dimension."

    # Retailer dimension checks
    assert len(retailer_dim) == 2, "Unexpected number of unique retailers."
    assert all('RetailerID' in retailer for retailer in retailer_dim), "Missing RetailerID in retailer dimension."

    # Date dimension checks
    assert len(date_dim) == 2, "Unexpected number of unique dates."
    assert all('Date' in date for date in date_dim), "Missing Date in date dimension."

    # Sales fact data checks
    assert len(sales_fact) == 2, "Unexpected number of sales records."
    assert all('SaleID' in sale for sale in sales_fact), "Missing SaleID in sales fact records."
    assert all(
        sale['ProductID'] in [product['ProductID'] for product in product_dim] for sale in sales_fact
    ), "Sales fact records reference invalid ProductID."
    assert all(
        sale['RetailerID'] in [retailer['RetailerID'] for retailer in retailer_dim] for sale in sales_fact
    ), "Sales fact records reference invalid RetailerID."
    assert all(
        sale['Date'] in [date['Date'] for date in date_dim] for sale in sales_fact
    ), "Sales fact records reference invalid Date."
