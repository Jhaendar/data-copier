import pandas as pd
import read


def load_table(name: str, conn):
    """loads the data using read module and inserts it to the table

    Args:
        name (str): table_name
        conn: connection string to the database
    """
    print(f"Loading data into table: {name}")
    table_path = read.get_table_path(name)

    if table_path is None:
        raise Exception(f"path for {name} does not exist!")

    table_reader = read.get_json_reader(table_path)

    for chunk in table_reader:
        chunk.to_sql(name, conn, if_exists="append", index=False)


def truncate_tables(conn):
    """truncates a table using pandas

    Args:
        conn (str): connection string to the database
    """
    print("Truncating tables:")
    for table in read.TABLE_NAMES:
        print(f"Truncating {table}")
        try:
            pd.read_sql(f"TRUNCATE {table}", conn)
        except Exception:
            pass


def load_all_tables(conn):
    """loads all the tables in read.TABLE_NAMES
    truncates the database tables before adding data

    Args:
        conn (str): connection string to the database
    """

    truncate_tables(conn)

    for table_name in read.TABLE_NAMES:
        load_table(table_name, conn)


if __name__ == "__main__":
    conn = "postgresql://retail_user:itversity@localhost:5452/retail_db"

    truncate_tables(conn)
    load_all_tables(conn)
    dep = pd.read_sql("SELECT * FROM orders", conn)
    print(dep.shape)
