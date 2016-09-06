#coding:utf8
import random
import MySQLdb
import time

class WriteData(object):
    def __init__(self, conn):
        self.conn = conn


    def writedb(self, callnumber,tagsrc,tag,tagnum):
        print callnumber
        try:
            check = self.check_callnumber_available(callnumber)
            if check != 1:
                self.insert_data(callnumber,tagsrc,tag,tagnum)
                print "insert"
            else:
                self.update_data(callnumber,tagsrc,tag,tagnum)
                print "update"
            self.conn.commit()
            time.sleep(0.01)

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
        #print len(rs)
        return len(rs)
        cursor.close()

    def insert_data(self, callnumber,tagsrc,tag,tagnum):
        cursor = self.conn.cursor()
        try:
            if tagnum != None:
                sql = "insert into spidertable(callnumber,migusrc,migutype,migucount) values('%s','%s','%s','%s')" %( callnumber,tagsrc,tag,tagnum)
            else:
                sql = "insert into spidertable(callnumber,migusrc,migutype) values('%s','%s','%s')" %( callnumber,tagsrc,tag)
            cursor.execute(sql)
            #print "insert_data :" + sql
        finally:
            cursor.close()

    def update_data(self, callnumber,tagsrc,tag,tagnum):
        cursor = self.conn.cursor()
        try:
            if tagnum != None:
                sql = "update spidertable set migusrc='%s',migutype='%s',migucount='%s' where callnumber='%s' "%(tagsrc,tag,tagnum,callnumber)
            else:
                sql = "update spidertable set migusrc='%s',migutype='%s' where callnumber='%s' " % (tagsrc, tag, callnumber)
            cursor.execute(sql)
        finally:
            cursor.close()


if __name__ == "__main__":


    conn = MySQLdb.Connect(host='10.2.46.170', user='root', passwd='111111', port=3306, db='spiderdb', charset='utf8')
    wr_data = WriteData(conn)

    try:
        f = open("miguoutput.txt","r")
        for line in f.readlines():
            line_list = line.split(" ")
            #print len(line_list)
            callnumber = line_list[0]
            tagsrc = line_list[1]
            tag = line_list[2]
            if len(line_list)>3:
                tagnum = line_list[3]
                wr_data.writedb(callnumber, tagsrc, tag, tagnum)
            else:
                wr_data.writedb(callnumber,tagsrc,tag,None)
        f.close()
    except Exception as e:
        print "出现问题," + str(e)
    finally:
        conn.close()
