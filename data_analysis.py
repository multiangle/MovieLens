
import pymysql
from matplotlib import pyplot as plt
import numpy as np
import math

def exe_query(cur, query):
    sz = cur.execute(query)

    for i in range(sz):
        print(i)
        cur.fetchone()

if __name__=='__main__':
    conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='admin', db='MovieLens')
    cur  = conn.cursor()
    query = "select * from ratings ;"
    exe_query(cur, query)
    cur.close()
    conn.close()
