# coding = UTF-8

import cx_Oracle
from config import config
import csv

from tables import tables

def oracle_execute(ora_connect, schema_name, table_name, table_cols):
    """выбирает из таблицы оракла данные
    вход:
    oracle_connect - подключение
    schema_name - схема таблицы
    table_name - имя таблицы
    выход:
    result - список строк из таблицы
    """

    cols = ','.join(table_cols)

    stmnt = '''select {cols} from
    {schema}.{table}'''.format(schema = schema_name,
                                table = table_name,
                                cols = cols).replace('    ','')

    cur = ora_connect.cursor()
    cur.execute(stmnt)
    result = cur.fetchall()

    return result


def write_csv(filename, headers, rows):
    """создаёт csv-файл с полученным именем
    и записывает в него полученные данные с полученными заголовками
    вход:
    filename - имя файла
    headers - заголовки файла (имена столбцов)
    rows - список строк
    """
    with open( 'out/' + filename + '.csv', 'w', encoding = 'utf-8', newline='') as csv_file:
        fields = headers
        writer = csv.DictWriter(csv_file, fields, delimiter=';')
        writer.writeheader()
        rows_dict = {}
        for item in rows:
            for thing_num, thing in enumerate(item):
                rows_dict[headers[thing_num]] = thing
            writer.writerow(rows_dict)

if __name__ == '__main__':

    ora_config = config['oracle']

    ora_dsn = cx_Oracle.makedsn( host = ora_config['host'],
                                    port = ora_config['port'],
                                    sid = ora_config['sid']
                                )
    ora_connect = cx_Oracle.connect(user = ora_config['user'],
                                    password = ora_config['pwd'],
                                    dsn = ora_dsn)

    for current_table in tables:
        cols = current_table['cols']
        table_name = current_table['table_name']
        result = oracle_execute(ora_connect,
                                current_table['schema_name'],
                                table_name,
                                current_table['cols']
                                )
        write_csv(table_name, cols, result)

    ora_connect.commit()
    ora_connect.close()
