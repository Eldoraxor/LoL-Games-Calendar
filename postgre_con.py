import pandas as pd
import psycopg2

def connector(database):
    connector_dir = {"host" : "localhost",
                    "port" : 5432,
                    "user" : "postgres",
                    "password" : "PGRom3821!",
                    "database" : database}
    return psycopg2.connect(**connector_dir)

def postgres_update_data(data_df : pd.DataFrame, table_name : str, keys : list):

    columns = data_df.columns.tolist()
    columns = ['\"' + col + '\"' for col in columns]
    columns_str = ', '.join(columns)
    keys = ', '.join(['\"' + key + '\"' for key in keys])
    placeholders = ', '.join(['%s'] * len(columns))

    insert_query = f"INSERT INTO public.{table_name} ({columns_str}) VALUES ({placeholders}) ON CONFLICT ({keys}) DO NOTHING;"

    data_to_insert = list(data_df.itertuples(index=False, name=None))

    with connector("LoL_games") as conn:
        with conn.cursor() as mycursor:
            mycursor.executemany(insert_query, data_to_insert)
        conn.commit()

def query_postgre(table : str, col_list : list):

    col_list = ['"' + col + '"' for col in col_list]
    col_str = ",".join(col_list) if len(col_list)>1 else col_list[0]
    query = f"SELECT {col_str} from {table}"

    with connector("LoL_games") as conn:
        with conn.cursor() as mycursor:
            mycursor.execute(query)
            query_result = mycursor.fetchall()
        conn.commit()

    return query_result