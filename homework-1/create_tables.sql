-- SQL-команды для создания таблиц
CREATE TABLE employees
(
    employee_id INT PRIMARY KEY,
    first_name  VARCHAR(255),
    last_name   VARCHAR(255),
    title       VARCHAR(255),
    birth_date  DATE,
    notes       TEXT
);

CREATE TABLE customers
(
    customer_id  VARCHAR(255) PRIMARY KEY,
    company_name VARCHAR(255),
    contact_name VARCHAR(255)
);

CREATE TABLE orders
(
    order_id    INT PRIMARY KEY,
    customer_id VARCHAR(255) REFERENCES customers (customer_id),
    employee_id INT REFERENCES employees (employee_id),
    order_date  DATE,
    ship_city   VARCHAR(255)
);
