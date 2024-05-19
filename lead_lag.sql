-- create sales table
CREATE TABLE sales (
    product VARCHAR(50),
    year INT,
    sales_amount DECIMAL(10,2)
);

-- Add some dummy data into the table
INSERT INTO sales (product, year, sales_amount) VALUES
('Apples', 2021, 1500.50),
('Bananas', 2021, 2500.75),
('Carrots', 2021, 3200.00),
('Apples', 2022, 1700.30),
('Bananas', 2022, 2900.20),
('Carrots', 2022, 3400.60),
('Apples', 2023, 1800.00),
('Bananas', 2023, 3100.45),
('Carrots', 2023, 3600.80),
('Oranges', 2021, 1100.25),
('Oranges', 2022, 1300.50),
('Oranges', 2023, 1400.75),
('Tomatoes', 2021, 1200.00),
('Tomatoes', 2022, 1500.35),
('Tomatoes', 2023, 1600.90);

-- LEAD Example
SELECT
    product,
    year,
    sales_amount,
    LEAD(sales_amount, 1, 0) OVER (
        ORDER BY year, product
    ) AS next_product_sales
FROM
    sales ;


-- LAG Example
SELECT
    product,
    year,
    sales_amount,
    LAG(sales_amount, 1, 0) OVER (
        ORDER BY year, product
    ) AS prev_product_sales
FROM
    sales ;

--Use Case
SELECT product, year, sales_amount,
    sales_amount - LAG(sales_amount, 1) OVER ( 
        PARTITION BY product
        ORDER BY year
    ) AS year_over_year_diff,
    LEAD(sales_amount, 1, 0) OVER (PARTITION BY product
        ORDER BY year) - sales_amount AS next_year_diff
FROM sales ORDER BY product, year;

