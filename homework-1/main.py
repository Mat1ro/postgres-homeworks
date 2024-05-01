"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password="postgres"
)

customers = "../homework-1/north_data/customers_data.csv"
employees = "../homework-1/north_data/employees_data.csv"
orders = "../homework-1/north_data/orders_data.csv"


def create_tuple_of_values(values: list):
    result = []
    for i in values:
        result.append(i)
    return tuple(result)


def create_sql_request(table_name, value):
    brackets = "(" + '%s, ' * len(value)
    brackets = brackets[:-2] + ")"
    request = f"INSERT INTO {table_name} VALUES {brackets}"
    return request


def save_table_from_csv(con, data_path, table_name):
    with con.cursor() as cur:
        with open(data_path, 'r') as csvfile:
            csvreader = list(csv.reader(csvfile))
            for i in range(1, len(list(csvreader))):
                values: tuple = create_tuple_of_values(csvreader[i])
                request: str = create_sql_request(table_name, values)
                cur.execute(request, values)
        conn.commit()


if __name__ == '__main__':
    save_table_from_csv(conn, orders, "orders")
