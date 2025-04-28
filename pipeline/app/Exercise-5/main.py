import psycopg2
import os
import csv


def create_tables(cur):
    cur.execute("DROP TABLE IF EXISTS transactions CASCADE;")
    cur.execute("DROP TABLE IF EXISTS products CASCADE;")
    cur.execute("DROP TABLE IF EXISTS customers CASCADE;")
    
    cur.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id INTEGER PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    address_1 VARCHAR(100),
                    address_2 VARCHAR(100),
                    city VARCHAR(50),
                    state VARCHAR(50),
                    zip_code INTEGER,
                    join_date DATE
                );
                
                CREATE INDEX idx_customers_name ON customers(last_name, first_name);
                """)
    
    cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    product_id INTEGER PRIMARY KEY,
                    product_code VARCHAR(10) UNIQUE NOT NULL,  
                    product_description VARCHAR(100)
                );
                
                CREATE INDEX idx_products_code ON products(product_code);
                """)
    
    cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id VARCHAR(100) PRIMARY KEY,
                    transaction_date DATE,
                    product_id INTEGER REFERENCES products(product_id),
                    product_code VARCHAR(10),
                    product_description VARCHAR(100),
                    quantity INTEGER,
                    account_id INTEGER REFERENCES customers(customer_id),
                    
                    CONSTRAINT fk_product FOREIGN KEY (product_id) 
                        REFERENCES products(product_id) ON DELETE RESTRICT,
                    CONSTRAINT fk_customer FOREIGN KEY (account_id) 
                        REFERENCES customers(customer_id) ON DELETE RESTRICT
                );
                
                CREATE INDEX idx_transactions_date ON transactions(transaction_date);
                CREATE INDEX idx_transactions_account ON transactions(account_id);
                CREATE INDEX idx_transactions_product ON transactions(product_id);
                """)


def ingest_data(cur, conn):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    
    with open(os.path.join(data_dir, "accounts.csv"), 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  
        for row in reader:
            cur.execute(
                """
                INSERT INTO customers (customer_id, first_name, last_name, address_1, address_2, city, state, zip_code, join_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                row
            )
    
    with open(os.path.join(data_dir, "products.csv"), 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  
        for row in reader:
            cur.execute(
                """
                INSERT INTO products (product_id, product_code, product_description)
                VALUES (%s, %s, %s)
                """,
                row
            )
    
    with open(os.path.join(data_dir, "transactions.csv"), 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  
        for row in reader:
            cur.execute(
                """
                INSERT INTO transactions (transaction_id, transaction_date, product_id, product_code, product_description, quantity, account_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                row
            )
    
    conn.commit()


def main():
    host = "postgres"
    database = "postgres"
    user = "airflow"
    pas = "airflow"
    
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
        cur = conn.cursor()
                
        create_tables(cur)
        
        ingest_data(cur, conn)
        
        cur.close()
        conn.close()
        print('Successful')        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")


if __name__ == "__main__":
    main()