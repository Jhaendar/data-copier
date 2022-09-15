from dotenv import load_dotenv
import os
import load


def main():
    load_dotenv()

    conn = get_connection_string()
    load.load_all_tables(conn)


def get_connection_string():
    DB_RETAIL_USER = os.getenv("DB_RETAIL_USER")
    DB_RETAIL_PASSWORD = os.getenv("DB_RETAIL_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")

    return f"postgresql://{DB_RETAIL_USER}:{DB_RETAIL_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


if __name__ == "__main__":
    main()
