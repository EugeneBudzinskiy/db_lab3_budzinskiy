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


def get_data_as_json(connection, table_name: str):
    query_template = "SELECT * FROM {}"
    df = execute_query(connection, sql_query=query_template.format(table_name))
    return df.to_json(path_or_buf=None)


def export_to_json(connection, table_names: tuple, filename: str = 'output', path: str = ''):
    ln = len(table_names)
    f_path = path + '/' + filename + '.json'
    with open(f_path, 'w') as f:
        f.write('{')

    for i in range(ln):
        res_json = get_data_as_json(connection=connection, table_name=table_names[i])
        s = '{}}}' if i == ln - 1 else '{},'
        with open(f_path, 'a') as f:
            d = f'"{table_names[i]}":{res_json}'
            f.write(s.format(d))


def main():
    wanted_tables = (
        'airlines',
        'airports',
        'flight_airline',
        'flight_route'
    )
    output_path = 'export_json'
    with connect_to_db() as c:
        export_to_json(connection=c, table_names=wanted_tables, path=output_path)


if __name__ == '__main__':
    main()
