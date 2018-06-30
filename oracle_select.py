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


def write_csv(table_name, headers, rows):
    """создаёт csv-файл с полученным именем
    и записывает в него полученные данные с полученными заголовками
    вход:
    table_name - имя таблицы
    headers - заголовки файла (имена столбцов)
    rows - список строк
    """
    filename = 'out/' + table_name + '.csv'
    with open(filename, 'w', encoding = 'utf-8', newline='') as csv_file:
        fields = headers
        writer = csv.DictWriter(csv_file, fields, delimiter=';')
        writer.writeheader()
        rows_dict = {}
        for item in rows:
            for thing_num, thing in enumerate(item):
                rows_dict[headers[thing_num]] = thing
            writer.writerow(rows_dict)


def write_ctl(table_name, cols, result):
    """создаёт для каждой таблицы ctl - файл
    и записывает в него контрольную информацию:
    количество столбцов
    количество строк
    количествуо столбцов, содержащих NULL
    вход:
    table_name - имя таблицы
    cols - заголовки файла (имена столбцов)
    result - список строк
    """
    rownum = len(result)
    colnum = len(cols)
    filename = 'out/' + table_name + '.ctl'
    fields = ['colnum', 'rownum', 'null_cols']
    null_cols = {}
    for col_num, col in enumerate(cols):
        null_cols[col] = 0
        for row in result:
            if row[col_num] == None:
                null_cols[col] = 1
                break

    null_cols_num = sum(null_cols.values())
    result_dict = {'colnum' : colnum,
                    'rownum' : rownum,
                    'null_cols' : null_cols_num}

    with open(filename, 'w', encoding = 'utf-8', newline='') as file:
        writer = csv.DictWriter(file, fields, delimiter=';')
        writer.writeheader()
        writer.writerow(result_dict)


if __name__ == '__main__':
    print('start!')

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
        write_ctl(table_name, cols, result)

    ora_connect.commit()
    ora_connect.close()

    print('end!')
