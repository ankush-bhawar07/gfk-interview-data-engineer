# Data Engineer Interview

👋 Welcome to the GfK Data Engineer Technical Test

## Objective
Write python code that will read data from `./generated-sales-data.csv`, process it and store it the postgres database using the provided schema. Then write SQL queries to answer the questions below in the *Tasks to be completed* section.

----
## Details
In this test you are given the following
- A python pipeline script `./src/pipeline.py` that orchastrates data pipeline steps in `./src/data_processing.py`
    - `pipeline.py` is complete and you should not need to modify it.
    - `data_processing.py` is incomplete and you will need to complete it. Function names and some signatures are provided for you. Please refer to pipeline.py for how the functions are used. This is where you should demonstrate your data processing and python skills.
- A CSV file `./generated-sales-data.csv` containing sales data. **NOTE: There are some issues with the data that you will need to handle in your `data_processing.py` functions**
- Tests for the `data_processing.py` functions in `./tests/test_data_processing.py`. These are here to help you test your code as you write it. Feel free to adapt / remove or add to these tests.


## Tasks to be completed and pushed to a public github repository
- Write python code to complete the `data_processing.py` functions
- Ensure pipeline.py can run without errors and publishes data to the postgres database
- Write SQL queries to answer the following questions and add them to a file `queries.sql` in the root of the repository

    1) **Total Sales by Product**
    Write a SQL query to calculate the total sales amount for each product, sorted by the total sales amount in descending order.

    2) **Sales by Month and Channel** 
    Write a SQL query to find the total sales amount and total quantity sold for each month and channel.

    3) **Top Selling Product by Category for Each Retailer**
    Write a SQL query to identify the top selling product by total sales amount in each category for each retailer.

- **Finally:** During the interview feedback session where you will present your solution we (gfk) will ask questions regarding deploying this type of pipeline with cloud based tooling such as Azure Microsoft Fabric or Google Cloud Platform Composer or Workflow. Please be prepared to discuss this.
---
## How to get started

### Setup python environment
#### Mac / Linux
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
#### Windows
```bash
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

### Start postgres
This creates a `sales` database
```bash
docker-compose up -d
```
*Note: if you have something running locally on port 5432 which will conflict with the postgres docker container then you can change the local port mapping in `docker-compose.yml` like so:*
```yaml
    ports:
      - "6543:5432"
``` 

### Load the provided schema into postgres
These commands use psql cli but you can connect to the database using the environment variables provided and use any SQL client you prefer.
#### Mac / Linux
```bash
PGHOST=localhost PGPORT=5432 PGDATABASE=sales PGUSER=postgres PGPASSWORD=mysecretpassword psql -f schema.sql
```

#### Windows
```bash
set PGHOST=localhost
set PGPORT=5432
set PGDATABASE=sales
set PGUSER=postgres
set PGPASSWORD=mysecretpassword
psql -f schema.sql
```

### Running the tests *(optional but may help in writing the python)*
```bash
pytest
```

### Running the pipeline
```bash
python src/pipeline.py ./generated_sales_data.csv
```

### Connect to the database
```bash
# Using psql as an example, you can use any SQL client you prefer
PGHOST=localhost PGPORT=5432 PGDATABASE=sales PGUSER=postgres PGPASSWORD=mysecretpassword psql
```
