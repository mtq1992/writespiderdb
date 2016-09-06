#coding:utf8

import MySQLdb


class WriteData(object):
    def __init__(self, conn):
        self.conn = conn


    def writedb(self, callnumber, tag, tagnum):
        print callnumber
        try:
            check = self.check_callnumber_available(callnumber)
            if check != 1:
                self.insert_data(callnumber,tag,tagnum)
                print "insert"
            else:
                self.update_data(callnumber,tag,tagnum)
                print "update"
            self.conn.commit()

        except Exception as e:
            print "rollback " + callnumber
            self.conn.rollback()
            #raise e 不再向上抛出异常


    def check_callnumber_available(self, callnumber):
        cursor = self.conn.cursor()

        sql = "select callnumber from spidertable where callnumber='%s'" % callnumber
        cursor.execute(sql)
        #print "check_callnumber_available " + sql
        rs = cursor.fetchall()
        print len(rs)
        return len(rs)
        cursor.close()

    def insert_data(self, callnumber, tag, tagnum):
        cursor = self.conn.cursor()
        try:
            sql = "insert into spidertable(callnumber,sogoutype,sogoucount) values('%s','%s','%s')" %(callnumber,tag,tagnum)
            cursor.execute(sql)
            #print "insert_data :" + sql
        finally:
            cursor.close()

    def update_data(self,callnumber,tag,tagnum):
        cursor = self.conn.cursor()
        try:
            sql = "update spidertable set sogoutype='%s',sogoucount='%s' where callnumber='%s' "%(tag,tagnum,callnumber)
            cursor.execute(sql)
        finally:
            cursor.close()


if __name__ == "__main__":


    conn = MySQLdb.Connect(host='10.2.46.170', user='root', passwd='111111', port=3306, db='spiderdb', charset='utf8')
    wr_data = WriteData(conn)

    try:
        f = open("sogououtput.txt","r")
        for line in f.readlines():
            line_list = line.split("\t")
            callnumber = line_list[0]
            tag = line_list[1]
            tagnum = line_list[2]
            wr_data.writedb(callnumber,tag,tagnum)
        f.close()
    except Exception as e:
        print "出现问题," + str(e)
    finally:
        conn.close()
