import os
import pandas as pd

RELATIVE_BASE = "retail_pg/retail_db_json"
TABLE_NAMES = [
    "departments",
    "categories",
    "customers",
    "order_items",
    "orders",
    "products",
]


def get_table_path(table_name: str, BASE_PATH: str = None) -> str:
    """returns the path to the file containing the data
    of the given table

    Args:
        table_name (str): table_name
        BASE_PATH (str, optional): the starting path.
            Defaults to Relative Base.

    Returns:
        str: path to the file containing table data
    """

    # Set default BASE_PATH
    if BASE_PATH is None:
        BASE_PATH = RELATIVE_BASE

    if table_name in os.listdir(BASE_PATH):
        file_path = os.listdir(f"{BASE_PATH}/{table_name}")
        return f"{BASE_PATH}/{table_name}/{file_path[0]}"
    else:
        return None


def get_json_reader(fp: str, chunks: int = 1000):
    """reads the json file, fp,
    and returns the chunked DataFrame of it

    Args:
        fp (str): file_path of json file
        chunks (int): chunk size. Default = 1000
    """

    return pd.read_json(fp, lines=True, chunksize=chunks)


if __name__ == "__main__":
    table_name = TABLE_NAMES[4]  # orders
    table_path = get_table_path(table_name)
    reader = get_json_reader(table_path)

    sum = 0

    for chunk in reader:
        sum += chunk.shape[0]
        print(f"{chunk.shape}; {sum}")
