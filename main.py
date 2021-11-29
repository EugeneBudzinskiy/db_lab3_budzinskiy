import psycopg2
import pandas as pd
from matplotlib import pyplot as plt


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


def create_views(connection):
    view_1 = '''
        CREATE VIEW flight_count_for_each_airline AS
            SELECT airlines.airline_id, COALESCE(tab.count, 0) AS airline_flight_count 
                FROM airlines LEFT JOIN (
                    SELECT airline_id, COUNT(*) FROM flight_airline LEFT JOIN airlines 
                        ON flight_airline.airline = airlines.airline_id GROUP BY airline_id
                    ) AS tab
                ON airlines.airline_id = tab.airline_id ORDER BY airlines.airline_id;
    '''
    view_2 = '''
        CREATE VIEW flight_count_from_each_airport AS
            SELECT airports.airport_id, COALESCE(tab.count, 0) AS flight_count
                FROM airports LEFT JOIN (
                    SELECT origin_airport, COUNT(*) FROM flight_route GROUP BY origin_airport
                ) AS tab
            ON airports.airport_id = tab.origin_airport ORDER BY airports.airport_id;	
    '''

    view_3 = '''
        CREATE VIEW flight_count_to_each_airport AS
            SELECT airports.airport_id, COALESCE(tab.count, 0) AS flight_count
    	        FROM airports LEFT JOIN (
    		        SELECT destination_airport, COUNT(*) FROM flight_route GROUP BY destination_airport
                ) AS tab
            ON airports.airport_id = tab.destination_airport ORDER BY airports.airport_id;
    '''

    execute_query(connection=connection, sql_query=view_1)
    execute_query(connection=connection, sql_query=view_2)
    execute_query(connection=connection, sql_query=view_3)


def get_visualisation(connection):
    query_1 = '''SELECT * FROM flight_count_for_each_airline;'''
    query_2 = '''SELECT * FROM flight_count_from_each_airport;'''
    query_3 = '''SELECT * FROM flight_count_to_each_airport;'''

    res_1 = execute_query(connection, query_1)
    res_1 = res_1.sort_values(by=res_1.columns[1], axis=0, ascending=False)
    res_1.plot(x=res_1.columns[0], y=res_1.columns[1], kind='bar')
    plt.show()

    res_2 = execute_query(connection, query_2)
    plt.pie(res_2[res_2.columns[1]], labels=res_2[res_2.columns[0]], autopct='%1.1f%%')
    plt.show()

    res_3 = execute_query(connection, query_3)
    res_3.plot(x=res_3.columns[0], y=res_3.columns[1], kind='bar')
    plt.show()


def main():
    with connect_to_db() as c:
        create_views(connection=c)
        get_visualisation(connection=c)


if __name__ == '__main__':
    main()
