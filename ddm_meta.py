# coding = UTF-8
import cx_Oracle
from oracle_lib import conn_oracle_db, close_oracle_db

def get_tables_list(stream_name, db_conf_name):
    """выбирает из таблицы оракла метаданные о таблицах по названию стрима
    вход:
    stream_name - название потока
    oracle_connect - подключение

    выход:
    result - список словарей формата tables:
        { 'schema_name' : '',
          'table_name' : '',
          'cols' : ['', '']
        }
    """

    ora_connect = conn_oracle_db(db_conf_name)

    stmnt_tables = "select t.id, t.schema_name, t.table_name " \
            "  from meta_tables t " \
            "  join set_stream_table st on st.table_id = t.id " \
            "  join set_stream	s on st.stream_id = s.id " \
            "  where s.stream_code = '{stream}'".format(stream = stream_name)

    cur_tables = ora_connect.cursor()
    cur_tables.execute(stmnt_tables)
    tables_without_cols = cur_tables.fetchall()

    tables_list = []
    for a_table in tables_without_cols:
        stmnt_cols = "select c.col_name " \
                     "  from meta_tables_cols c " \
                     " where c.table_id = {table_id} " \
                     " order by c.col_position ".format(table_id = a_table[0])

        cur_cols = ora_connect.cursor()
        cur_cols.execute(stmnt_cols)
        cols_tuple = cur_cols.fetchall()

        cols_list = []
        for a_col in cols_tuple:
            cols_list.append(a_col[0])

        a_table_dict = {'schema_name' : a_table[1],
                        'table_name' : a_table[2],
                        'cols' : cols_list}
        tables_list.append(a_table_dict)

    close_oracle_db(ora_connect)
    return tables_list

if __name__ == '__main__':
    print('start')

    # проверка процедуры get_tables_list
    tables = get_tables_list('stream001', 'ddm_meta')
    print(tables)

    print('end')
