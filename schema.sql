-- Create Product Dimension Table
CREATE TABLE IF NOT EXISTS Product_Dim (
    ProductID SERIAL PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Brand VARCHAR(255),
    Category VARCHAR(255)
);

-- Create Retailer Dimension Table
CREATE TABLE IF NOT EXISTS Retailer_Dim (
    RetailerID SERIAL PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    Channel VARCHAR(50),
    Location VARCHAR(255)
);

-- Create Date Dimension Table
CREATE TABLE IF NOT EXISTS Date_Dim (
    Date DATE PRIMARY KEY,
    Day INTEGER NOT NULL,
    Month INTEGER NOT NULL,
    Year INTEGER NOT NULL,
    Quarter INTEGER NOT NULL,
    DayOfWeek VARCHAR(9) NOT NULL,
    WeekOfYear INTEGER NOT NULL
);

-- Create Sales Fact Table
CREATE TABLE IF NOT EXISTS Sales_Fact (
    SaleID SERIAL PRIMARY KEY,
    ProductID INTEGER REFERENCES Product_Dim(ProductID),
    RetailerID INTEGER REFERENCES Retailer_Dim(RetailerID),
    Date DATE REFERENCES Date_Dim(Date),
    Quantity INTEGER NOT NULL,
    Price NUMERIC NOT NULL
);
