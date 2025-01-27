CREATE DATABASE Portfolio;

USE Portfolio;

CREATE TABLE sales (
    sale_id INT PRIMARY KEY IDENTITY(1,1), -- Unique identifier for each sale
    region VARCHAR(50) NOT NULL,            -- Region where the sale occurred
    salesperson VARCHAR(100) NOT NULL,     -- Name of the salesperson
    total_sales DECIMAL(10, 2) NOT NULL,   -- Total sales amount (e.g., 12345.67)
    sale_date DATE                         
);


SELECT * FROM sales;

INSERT INTO sales (region, salesperson, total_sales, sale_date)
VALUES
('North America', 'Alice Johnson', 15000.00, '2025-01-01'),
('Europe', 'Bob Smith', 20000.00, '2025-01-02'),
('Asia', 'Chris Lee', 18000.00, '2025-01-03'),
('North America', 'Diana Prince', 22000.00, '2025-01-04'),
('Europe', 'Ethan Hunt', 19000.00, '2025-01-05');

-- query to find the salesperson with highest sales in each region

WITH RegionSales AS(
	SELECT region, salesperson, total_sales,
		RANK() OVER(PARTITION BY region ORDER BY total_sales DESC) as rank
		FROM sales
)

SELECT region, salesperson, total_sales
FROM RegionSales
WHERE rank=1;




CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    department_id INT,
    salary DECIMAL(10, 2)
);

INSERT INTO employees (employee_id, department_id, salary) VALUES
(1, 101, 90000.00),
(2, 101, 75000.00),
(3, 101, 75000.00),
(4, 102, 80000.00),
(5, 102, 70000.00),
(6, 101, 60000.00),
(7, 102, 90000.00),
(8, 103, 95000.00),
(9, 103, 85000.00),
(10, 103, 85000.00);

SELECT employee_id, department_id, salary,
       ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS row_num
FROM employees;


SELECT employee_id, department_id, salary,
       RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rank
FROM employees;


SELECT employee_id, department_id, salary,
       DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS dense_rank
FROM employees;

SELECT employee_id, department_id, salary,
       NTILE(2) OVER (PARTITION BY department_id ORDER BY salary DESC) AS tile
FROM employees;


SELECT employee_id, department_id, salary
FROM (
    SELECT employee_id, department_id, salary,
           RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rank
    FROM employees
) ranked_employees
WHERE rank <= 3;




