
import pymysql

def fetchTableData(tb_name, batch_size=None):
    conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='admin', db='MovieLens')
    cur  = conn.cursor()
    if not batch_size:
        # fetch all data
        query = "select * from {}".format(tb_name)
        cur.execute(query)
        res = cur.fetchall()
        return res
    else:
        pass

if __name__=="__main__":
    ret = fetchTableData("tags")
    print(ret)



