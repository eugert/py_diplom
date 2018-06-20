import cx_Oracle
from config import config

if __name__ == '__main__':
    oracle_connect = config['oracle']

    ora_dsn = cx_Oracle.makedsn( host = oracle_connect['host'],
                                    port = oracle_connect['port'],
                                    # service_name = oracle_connect['service_name']
                                    sid = oracle_connect['sid']
                                )
    ora_connect = cx_Oracle.connect(user = oracle_connect['user'],
                                    password = oracle_connect['pwd'],
                                    dsn = ora_dsn)

    stmnt = '''select userenv('LANGUAGE') from dual'''

    print(stmnt)

    cur = ora_connect.cursor()
    cur.execute(stmnt)
    result = cur.fetchall()
    print(result)

    ora_connect.commit()
    ora_connect.close()
