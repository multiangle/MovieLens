
import pymysql
import csv
import re

# 读取
# genome-score.csv
# genonme-tags.csv
# links.csv
# movies.csv
# ratings.csv
# tags.csv

def import_movies(path):
    conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='admin', db='MovieLens')
    cur = conn.cursor()
    cmd_pattern = "insert into {} values ({})"
    batch_size = 1
    cmd_pool = []
    with open(path, 'r') as f:
        reader = csv.reader(f)
        for i, line in enumerate(reader):
            if i==0:
                l = ["%s"]*(len(line)+1)
                cmd_pattern = cmd_pattern.format("movies", ",".join(l))
                print(cmd_pattern)
            else:
                # cmd = cmd_pattern.format(*line)
                tmp = re.findall(r'\(.*?\)', line[1])
                year = None
                if len(tmp)>0:
                    # print(tmp)
                    year = re.findall(r'\d+', tmp[-1])
                    if len(year)>0:
                        year = year[0]
                    else:
                        year = None
                keys = line[-1].split('|')
                for key in keys:
                    n_line = []
                    n_line.append(line[0])
                    n_line.append(line[1])
                    n_line.append(year)
                    n_line.append(key)
                    cmd_pool.append(tuple(n_line))
                # print(line)
                if len(cmd_pool)>=batch_size:
                    # print(cmd_pool)
                    try:
                        # print(cmd_pool)
                        cur.executemany(cmd_pattern, cmd_pool)
                        conn.commit()
                    except Exception as e:
                        print("error")
                        print(cmd_pool)
                        print(e)
                    cmd_pool = []
                    # print("5000")

    if len(cmd_pool)>0:
        cur.executemany(cmd_pattern, cmd_pool)
        conn.commit()
    cur.close()
    conn.close()

def import_files(path, table_name):
    conn = pymysql.connect(host="127.0.0.1", port=3306, user='root', passwd='admin', db='MovieLens')
    cur = conn.cursor()
    cmd_pattern = "insert into {} values ({})"
    batch_size = 5
    cmd_pool = []
    with open(path, 'r') as f:
        reader = csv.reader(f)
        for i, line in enumerate(reader):
            if i==0:
                l = ["%s"]*len(line)
                cmd_pattern = cmd_pattern.format(table_name, ",".join(l))
                print(cmd_pattern)
            else:
                # print(line)
                # cmd = cmd_pattern.format(*line)
                cmd_pool.append(tuple(line))
                # print(line)
                if len(cmd_pool)==batch_size:
                    # print(cmd_pool)
                    try:
                        cur.executemany(cmd_pattern, cmd_pool)
                        conn.commit()
                    except:
                        print("error")
                    cmd_pool = []
                    # print("5000")

    if len(cmd_pool)>0:
        cur.executemany(cmd_pattern, cmd_pool)
        conn.commit()
    cur.close()
    conn.close()

if __name__=="__main__":
    path = "/media/multiangle/Software/recsys data/MovieLens/ml-20m/"
    name = "tags.csv"
    tb_name = "_".join(name.split("-")).split(".")[0]
    # import_files(path+name, tb_name)
    import_movies(path+"movies.csv")