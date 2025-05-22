import psycopg2
import logging

logging.basicConfig(
    format="{asctime} | {levelname} | {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO
)

def create_tables(conn):
    cmds = [
        """
        CREATE TABLE IF NOT EXISTS accounts (
            customer_id INTEGER PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            address_1 VARCHAR(100) NOT NULL,
            address_2 VARCHAR(100),
            city VARCHAR(50) NOT NULL,
            state VARCHAR(20) NOT NULL,
            zip_code VARCHAR(5) NOT NULL,
            join_date DATE NOT NULL
        );
        """,

        "CREATE INDEX IF NOT EXISTS accounts_city_idx ON accounts (city);",
        "CREATE INDEX IF NOT EXISTS accounts_state_idx ON accounts (state);",
        
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            product_code INTEGER NOT NULL,
            product_description VARCHAR(255) NOT NULL
            CONSTRAINT unique_product_code UNIQUE (product_code)
        );
        """,

        "CREATE INDEX IF NOT EXISTS products_code_idx ON products (product_code);",

        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(30) PRIMARY KEY,
            transaction_date DATE NOT NULL,
            product_id INTEGER NOT NULL,
            product_code INTEGER NOT NULL,
            product_description VARCHAR(255) NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            account_id INTEGER NOT NULL REFERENCES accounts (customer_id),
            CONSTRAINT fk_product FOREIGN KEY (product_id, product_code)
                REFERENCES products (product_id, product_code)
        );
        """,

        "CREATE INDEX IF NOT EXISTS transactions_date_idx ON transactions (transaction_date)",
        "CREATE INDEX IF NOT EXISTS transactions_account_idx ON transactions (account_id)",
        "CREATE INDEX IF NOT EXISTS transactions_product_idx ON transactions (product_id)",
    ]
    
    try:
        with conn.cursor() as cur:
            for cmd in cmds:
                cur.execute(cmd)
        conn.commit()
        logging.info("Successfully created tables.")
    except psycopg2.Error as e:
        logging.error(f"Error creating tables: {e}")
        cur.rollback()
        raise
    

def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)



if __name__ == "__main__":
    main()
