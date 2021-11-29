import psycopg2
from sqlalchemy import create_engine
import pandas as pd


def connect_to_db(connect_data_src: str = 'connection.ini'):
    with open(connect_data_src) as f:
        connect_data = dict(map(lambda x: (x.replace(' ', '').replace('\n', '').split(':')), f.readlines()))
        connection = psycopg2.connect(**connect_data)
    return connection


def create_engine_for_db(connect_data_src: str = 'connection.ini'):
    with open(connect_data_src) as f:
        connect_data = dict(map(lambda x: (x.replace(' ', '').replace('\n', '').split(':')), f.readlines()))
        engine_str = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(**connect_data)
    return create_engine(engine_str)


def open_csv(filename: str, use_cols: list = None, use_names: list = None) -> pd.DataFrame:
    df = pd.read_csv(filepath_or_buffer=filename, usecols=use_cols)
    if use_names is not None:
        df.columns = use_names
    return df


def import_into_table(engine, table_name: str, filename: str, use_cols: list = None, use_names: list = None):
    df = open_csv(filename=filename, use_cols=use_cols, use_names=use_names)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)


def main():
    airlines_csv_path = "data/airlines.csv"
    airlines_get_cols = ['IATA_CODE', 'AIRLINE']
    airlines_set_names = ['airline_id', 'airline_name']

    airports_csv_path = "data/airports.csv"
    airports_get_cols = ['IATA_CODE', 'AIRPORT']
    airports_set_names = ['airport_id', 'airport_name']

    with connect_to_db() as c:
        engine = create_engine_for_db()
        import_into_table(
            engine=engine,
            table_name='airlines',
            filename=airlines_csv_path,
            use_cols=airlines_get_cols,
            use_names=airlines_set_names
        )
        import_into_table(
            engine=engine,
            table_name='airports',
            filename=airports_csv_path,
            use_cols=airports_get_cols,
            use_names=airports_set_names
        )


if __name__ == '__main__':
    main()
