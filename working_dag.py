# coding = UTF-8

import os, shutil

import cx_Oracle
import csv
import pysftp
from datetime import date


from config import config
#from tables import tables
from oracle_lib import conn_oracle_db, close_oracle_db
from ddm_meta import get_tables_list

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


def write_csv(file_name, headers, rows):
    """создаёт csv-файл с полученным именем и записывает в него
    полученные данные с полученными заголовками
    вход:
        file_name - имя таблицы
        headers - заголовки файла (имена столбцов)
        rows - список строк
    """
    with open(file_name + '.csv', 'w', encoding = 'utf-8', newline='') as csv_file:
        fields = headers
        writer = csv.DictWriter(csv_file, fields, delimiter=';')
        writer.writeheader()
        rows_dict = {}
        for item in rows:
            for thing_num, thing in enumerate(item):
                rows_dict[headers[thing_num]] = thing
            writer.writerow(rows_dict)


def write_ctl(file_name, cols, result):
    """создаёт для каждой таблицы ctl - файл и записывает в него контрольную информацию:
        - количество столбцов
        - количество строк
        - количествуо столбцов, содержащих NULL
    вход:
        file_name - имя таблицы
        cols - заголовки файла (имена столбцов)
        result - список строк
    """
    rownum = len(result)
    colnum = len(cols)
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

    with open(file_name + '.ctl', 'w', encoding = 'utf-8', newline='') as file:
        writer = csv.DictWriter(file, fields, delimiter=';')
        writer.writeheader()
        writer.writerow(result_dict)


def write_tkt(pkg_name, table_name):
    with open(pkg_name + '.tkt', 'a', encoding = 'utf-8',
                newline='') as file:
        file.write(table_name + '.csv\n')


if __name__ == '__main__':
    print('start')

    # имя пакета и каталога
    stream_name = 'stream001'
    dt_today = date.today()
    # pkg_name = 'test_pkg'
    pkg_name = 'pkg_' + stream_name + '_' + dt_today.strftime('%Y%m%d')
    out_folder = 'out/' + pkg_name + '/'
    # print('out_folder = ', out_folder)

    # получение списка таблиц
    tables = get_tables_list(stream_name, 'ddm_meta')
    # print('tables = ', tables)

    # удаление папки, если она уже есть и создание папки,
    # если её нет
    if os.path.exists(out_folder):
        shutil.rmtree(out_folder)
        os.makedirs(out_folder)
    else:
        os.makedirs(out_folder)
    print('create out_folder ', out_folder)


    # подхватывание конфига из файла и создание подключений
    ora_connect = conn_oracle_db('oracle')

    # потабличный запуск процедур
    for current_table in tables:
        cols = current_table['cols']
        table_name = current_table['table_name']
        result = oracle_execute(ora_connect,
                                current_table['schema_name'],
                                table_name,
                                current_table['cols']
                                )
        write_csv(out_folder + table_name, cols, result)
        write_ctl(out_folder + table_name, cols, result)
        write_tkt(out_folder + pkg_name, table_name)

    # закрытие подключенией
    ora_connect.commit()
    ora_connect.close()

    print('loaded package')

    # !!!Текущий!!!Перенос на sftp
    # cnopts = pysftp.CnOpts()
    # cnopts.hostkeys = None
    # with pysftp.Connection('185.188.183.220', username='anton', password='swJml52410Cj', cnopts=cnopts) as sftp:
    #     with sftp.cd('/opt/diplom_upload/'):
    #         print('pwd: ', sftp.pwd)
    #         sftp.put_r(out_folder, '/opt/diplom_upload/' + pkg_name)
    # print('put sftp')

    # Перенос на sftp
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection('185.188.183.220', username='anton', password='swJml52410Cj', cnopts=cnopts) as sftp:
        sftp.put_r(out_folder, '/opt/diplom_upload/' + pkg_name)
    print('put sftp')

    print('end')
