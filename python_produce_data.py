import mysql.connector
import uuid
from datetime import datetime, timezone
from random import randint
import random
import datetime
from time import sleep
# MySQL container connection details
db_config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "debezium" 
    # "database": "inventory" 
}

try:
    # Connect to the MySQL server
    connection = mysql.connector.connect(**db_config)
    print("Connected to MySQL")

    # Create a cursor to interact with the database
    cursor = connection.cursor()

    # Create a new database
    # new_database = "oms" 
    # cursor.execute(f"CREATE DATABASE IF NOT EXISTS {new_database}")
    # print(f"Database '{new_database}' created or already exists")
    # Use the new database
    # cursor.execute(f"USE {new_database}")

    # Create a new table
    table_name = "sales"
    # states = ("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
    #           "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    #           "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA",
    #           "WA", "WV", "WI", "WY")
    shipping_types = ("Free", "3-Day", "2-Day")
    set_types = ("java","c++","python")
    gender_types = ("M","F")

    categories = ("Garden", "Kitchen", "Office", "Household")
    db = 'oms'
    for i in range(1,3):
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db}{i}")

        # query=f"""create table if not exists {db}{i}.orders (order_id bigint, units int, price float(5,2), primary key (order_id))"""
        # cursor.execute(query)
        # for j in range(0,5):
        #     order_id = random.randint(1, 200000)
        #     units = random.randint(1, 100)
        #     price = random.randint(1, 100) * 1.2
        #     data_order = (order_id, units, price)
        #     query = f"""INSERT INTO {db}{i}.orders
        #                                     (
        #                                     order_id, units,price
        #                                     )
        #                                 VALUES (%s,%s,%s)"""
        #     cursor.execute(query,data_order)

        # query=f"""create table if not exists {db}{i}.orders_skus (order_skus_id bigint, units int, price float(5,2), primary key (order_skus_id))"""
        # cursor.execute(query)
        # for j in range(0,5):
        #     order_id = random.randint(1, 200000)
        #     units = random.randint(1, 100)
        #     price = random.randint(1, 100) * 1.2
        #     data_order = (order_id, units, price)
        #     query = f"""INSERT INTO {db}{i}.orders_skus
        #                                     (
        #                                     order_skus_id, units,price
        #                                     )
        #                                 VALUES (%s,%s,%s)"""
        #     cursor.execute(query,data_order)
        
        query=f"""create table if not exists {db}{i}.sales (sale_id bigint, item_id int, price float(5,2), primary key (sale_id))"""
        cursor.execute(query)
        for j in range(0,1):
            sale_id = random.randint(1, 200000)
            item_id = random.randint(1, 100)
            price = random.randint(1, 100) * 1.2
            data_order = (sale_id, item_id, price)
            query = f"""INSERT INTO {db}{i}.sales
                                            (
                                            sale_id, item_id,price
                                            )
                                        VALUES (%s,%s,%s)"""
            cursor.execute(query,data_order)

        # query=f"""create table if not exists {db}{i}.sales_inventory (sale_inventory_id bigint, item_id int, price float(5,2), primary key (sale_inventory_id))"""
        # cursor.execute(query)
        # for j in range(0,5):
        #     sale_id = random.randint(1, 200000)
        #     item_id = random.randint(1, 100)
        #     price = random.randint(1, 100) * 1.2
        #     data_order = (sale_id, item_id, price)
        #     query = f"""INSERT INTO {db}{i}.sales_inventory
        #                                     (
        #                                     sale_inventory_id, item_id,price
        #                                     )
        #                                 VALUES (%s,%s,%s)"""
        #     cursor.execute(query,data_order)
    
    # create_table_query = f"""
    # CREATE TABLE IF NOT EXISTS {table_name} (
    #     invoice_id bigint ,
    #     item_id int,
    #     smallint_col smallint,
    #     mediumint_col mediumint,
    #     quantity tinyint,
    #     category VARCHAR(255),
    #     gender char(1),
    #     price decimal(20,2),
    #     price1 float(5,2),
    #     price2 double(10,2),
    #     order_date timestamp,
    #     current_dt datetime,
    #     shipping_type ENUM("Free", "3-Day", "2-Day"),
    #     json_col JSON,
    #     set_col SET("java","c++","python"),
    #     tinytext_col TINYTEXT,
    #     text_col TEXT,
    #     mediumtext_col MEDIUMTEXT,
    #     longtext_col LONGTEXT,
    #     dob date,
    #     start_to_work time,
    #     year_col year,
    #     PRIMARY KEY (invoice_id)
    # )
    # """ 
    # cursor.execute(create_table_query)
    # print(f"Table '{table_name}' created or already exists")


    # # Insert transactions
    # cnt=0
    # for i in range(0,10):
    #     cnt = cnt + 1    
    #     invoice_id = random.randint(1, 200000)
    #     item_id = random.randint(1, 1000)
    #     smallint_col = random.randint(1,100)
    #     mediumint_col = random.randint(1,1000)
    #     quantity = random.randint(1, 10)
    #     category = categories[random.randint(0, len(categories) - 1)]
    #     gender = gender_types[random.randint(0, len(gender_types) - 1)]
    #     price = random.randint(1, 100) * 1.2
    #     price1 = 7.3
    #     price2 = 5.67
    #     order_date = datetime.date(2016, random.randint(1, 12), random.randint(1, 28)).isoformat()
    #     current_dt = '2023-09-08'
    #     shipping_type = shipping_types[random.randint(0, len(shipping_types) - 1)]
    #     json_col = '{"key1":10,"key2":20}'
    #     set_col = set_types[random.randint(0, len(set_types) - 1)]
    #     tinytext_col = 'I am a tinytext column'
    #     text_col = 'I am a text column'
    #     mediumtext_col = 'I am a mediumtext column'
    #     longtext_col = 'I am a longtext column'
    #     dob = '2023-09-11'
    #     start_to_work = '08:00:00'
    #     year_col = '2023'


        
    #     # state = states[random.randint(0, len(states) - 1)]

    #     data_order = (invoice_id, item_id, smallint_col,mediumint_col, quantity,category,gender, price, price1, price2, order_date, current_dt,shipping_type,json_col,set_col,tinytext_col,text_col,mediumtext_col,longtext_col,dob,start_to_work,year_col)
    #     print(data_order)

    #     query = """INSERT INTO sales
    #                                         (
    #                                         invoice_id, item_id, smallint_col, mediumint_col,quantity,category, gender,price, price1, price2, order_date, current_dt, shipping_type,json_col,set_col,tinytext_col,text_col,mediumtext_col,longtext_col,dob,start_to_work,year_col
    #                                         )
    #                                     VALUES (%s,%s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)"""
    #     cursor.execute(query,data_order)
    #     print("*************************************************************")
    #     print(f"**********************  inserted {cnt} Row/s **********************")
        # sleep(5)
        # print("sleeping")


    # Upsert transactions
    # upsert_query = f"""
    # INSERT INTO {table_name} (name, email, age) VALUES (%s, %s, %s)
    # ON DUPLICATE KEY UPDATE name=VALUES(name), age=VALUES(age)
    # """
    # data = [
    #     ("John Doe", "john@example.com", 30),
    #     ("Jane Smith", "jane@example.com", 28)
    # ]
    # cursor.executemany(upsert_query, data)
    connection.commit()
    # print("Upsert transactions completed")
    
    # cursor.execute("select * from users;")
    # result = cursor.fetchall()
    # for row in result:
    #     print(row)

except mysql.connector.Error as err:
    print("Error:", err)

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if connection.is_connected():
        connection.close()
        print("MySQL connection closed")
    