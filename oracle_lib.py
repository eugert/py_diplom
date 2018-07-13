# coding = UTF-8

import cx_Oracle
from config import config

def conn_oracle_db (db_conf_name):
    """ подхватывание конфига из файла и создание подключений """

    ora_config = config[db_conf_name]
    ora_dsn = cx_Oracle.makedsn(host=ora_config['host'],
                                port=ora_config['port'],
                                sid=ora_config['sid']
                                )
    ora_connect = cx_Oracle.connect(user=ora_config['user'],
                                    password=ora_config['pwd'],
                                    dsn=ora_dsn)
    return ora_connect


def close_oracle_db (ora_connect):
    """ закрытие подключенией """
    ora_connect.commit()
    ora_connect.close()