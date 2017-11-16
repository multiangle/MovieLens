
import pymysql
from matplotlib import pyplot as plt
import numpy as np
import math

def exe_query(cur, query):
    cur.execute(query)
    res = cur.fetchall()
    u_num = len(res)
    print(u_num)
    print(min([x[1] for x in res]))

if __name__=='__main__':
    conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='admin', db='MovieLens')
    cur  = conn.cursor()
    query = "select * from ratings order by timestamp limit 1000 ;"
    query ="select * from u1"
    exe_query(cur, query)
    cur.close()
    conn.close()
