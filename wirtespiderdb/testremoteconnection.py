import MySQLdb

try:
    conn = MySQLdb.connect(host='10.2.46.170',
                           port=3306,
                           user='root',
                           passwd='111111',
                           db='spiderdb',
                           charset='utf8'
                           )
    cursor = conn.cursor()
    sql_select = "select * from spidertable"
    cursor.execute(sql_select)
    rs = cursor.fetchall()
    print len(rs)
except Exception as e:
    print "error"+str(e)