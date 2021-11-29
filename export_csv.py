import psycopg2
import pandas as pd


def connect_to_db(connect_data_src: str = 'connection.ini'):
    with open(connect_data_src) as f:
        connect_data = dict(map(lambda x: (x.replace(' ', '').replace('\n', '').split(':')), f.readlines()))
        connection = psycopg2.connect(**connect_data)
    return connection


def execute_query(connection, sql_query) -> pd.DataFrame:
    buff = list()
    with connection.cursor() as curs:
        curs.execute(sql_query)
        if curs.description is not None:
            col_names = [desc[0] for desc in curs.description]
            for row in curs:
                buff.append(tuple(map(lambda x: x.replace(' ', '') if type(x) is str else x, row)))
    return None if len(buff) == 0 else pd.DataFrame(buff, columns=col_names)


def single_export_to_csv(connection, table_name: str, path: str = ''):
    query_template = "SELECT * FROM {}"
    df = execute_query(connection, sql_query=query_template.format(table_name))
    p = None if path is None else path + '/' + table_name + '.csv'
    df.to_csv(path_or_buf=p, index=False)


def export_to_csv(connection, table_names: tuple, path: str = ''):
    for el in table_names:
        single_export_to_csv(connection=connection, table_name=el, path=path)


def main():
    wanted_tables = (
        'airlines',
        'airports',
        'flight_airline',
        'flight_route'
    )
    output_path = 'export_csv'
    with connect_to_db() as c:
        export_to_csv(connection=c, table_names=wanted_tables, path=output_path)


if __name__ == '__main__':
    main()
