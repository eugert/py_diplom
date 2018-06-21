# coding = UTF-8

from argparse import ArgumentParser
import cx_Oracle
from config import config
import csv

from tables import tables

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-t', '--table_name')
    parser.add_argument('-sch', '--schema_name')
    return parser.parse_args()


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
    with open(filename + '.csv', 'w', encoding = 'utf-8', newline='') as csv_file:
        fields = headers
        writer = csv.DictWriter(csv_file, fields, delimiter=';')
        writer.writeheader()
        rows_dict = {}
        print(rows)
        for item in rows:
            print('item: ', item)
            for thing_num, thing in enumerate(item):
                print('thing: ', thing)
                print('thing_num: ', thing_num)
                # print('headers[thing_num]: ', headers[thing_num])
                rows_dict[headers[thing_num]] = thing
            print('current_rows_dict: ', rows_dict)
            writer.writerow(rows_dict)

        print('rows_dict: ', rows_dict)

        # for one_row in rows_dict:
        #     print('\n','current_item:', type(one_row), one_row, '\n' )
            # print('answers_dict[one_row]:', answers_dict[one_row])
            # writer.writerow(one_row)

if __name__ == '__main__':
    # args = parse_args()
    # table_name = args.table_name
    # schema_name = args.schema_name

    ora_config = config['oracle']

    ora_dsn = cx_Oracle.makedsn( host = ora_config['host'],
                                    port = ora_config['port'],
                                    sid = ora_config['sid']
                                )
    ora_connect = cx_Oracle.connect(user = ora_config['user'],
                                    password = ora_config['pwd'],
                                    dsn = ora_dsn)

    cols = tables['cols']
    table_name = tables['table_name']
    result = oracle_execute(ora_connect, tables['schema_name'],
                            table_name, tables['cols'])

    # print(cols, '\n', result)
    # for row in result:
    #     print(row)

    write_csv(table_name, cols, result)

    ora_connect.commit()
    ora_connect.close()
