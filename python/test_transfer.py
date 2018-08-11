#coding:utf8
import sys
import MySQLdb

conn = MySQLdb.Connect(
    host = "127.0.0.1",
    port = 3306,
    user = "root",
    passwd = "casa",
    db = "imooc",
    charset = "utf8"    
    )
cursor = conn.cursor()
sql_creat = "CREATE TABLE account (\
            acctid INT(11) DEFAULT NULL COMMENT 'account id',\
            money INT(11) DEFAULT NULL COMMENT 'remaining sum'\
            ) ENGINE = INNODB DEFAULT CHARSET = utf8;" 
print sql_creat
sql_show = "SHOW COLUMNS FROM account; " 
try:
    cursor.execute("SHOW TABLES;")
    print cursor.fetchall()
    cursor.execute("DROP TABLE IF EXISTS account;")          
    cursor.execute(sql_creat)
    cursor.execute("INSERT INTO account VALUES ('566','110'),('567','110');")    
    cursor.execute(sql_show)
    rs = cursor.fetchall()
    print rs
    conn.commit()
except Exception as err:
    print err
    conn.rollback()
    

cursor.close()
conn.close()

class TransferMoney(object):
    def __init__(self,conn):
        self.conn = conn
    

    def check_acct_available(self, acctid):
        cursor = self.conn.cursor()
        try:
            sql = "SELECT * FROM account WHERE acctid = %s;" % acctid
            cursor.execute(sql)
            print "check_acct_available:"+sql
            rs = cursor.fetchall()
            if len(rs) != 1:
                raise Exception("The account do not exists",acctid) 
        finally:
            cursor.close()
    
    
    def check_has_enough_money(self, acctid, money):
        cursor = self.conn.cursor()
        try:
            sql = "SELECT * FROM account WHERE acctid = %s and money>%s;" % (acctid,money)
            print cursor.execute(sql),">111"
            print "check_has_enough_money:"+sql
            rs = cursor.fetchall()
            print rs
            if len(rs) != 1:
                raise Exception("check_has_enough_money->fail",acctid) 
        finally:
            cursor.close() 

    def reduce_money(self, acctid, money):
        cursor = self.conn.cursor()        
        try:
            sql = "UPDATE account SET money = money - %s WHERE acctid = %s;" % (money,acctid)
#             print cursor.execute(sql),">???"
#             print "reduce_money:" + sql
#             rs = cursor.fetchall()
#             print rs
            if cursor.execute(sql) != 1:
                raise Exception("reduce_money->fail,'acctid='",acctid) 
        finally:
            cursor.close()                
    
    def add_money(self, acctid, money):
        cursor = self.conn.cursor()        
        try:
            sql = "UPDATE account SET money = money + %s WHERE acctid = %s;" % (money,acctid)
#             cursor.execute(sql)
#             print "add_money:" + sql
#             rs = cursor.fetchall()
#             print rs
            if cursor.execute(sql) != 1:
                raise Exception("add_money->fail,'acctid'=",acctid) 
        finally:
            cursor.close() 

    
    
    def transfer(self,source_acctid,target_acctid,money):
        try:
            self.check_acct_available(source_acctid)
            self.check_acct_available(target_acctid)
            self.check_has_enough_money(source_acctid,money)
            self.reduce_money(source_acctid,money)
            self.add_money(target_acctid,money)            
            self.conn.commit()
        except Exception as err:
            self.conn.rollback()
            raise err
            
if __name__ == "__main__":
    source_acctid = 566
    target_acctid = 567
    money = 100

    conn = MySQLdb.Connect(
    host = "127.0.0.1",
    port = 3306,
    user = "root",
    passwd = "casa",
    db = "imooc",
    charset = "utf8"
    )

 
    tr_money = TransferMoney(conn)
    try:
        tr_money.transfer(source_acctid,target_acctid,money)
    except Exception as err:
        print "issue was happend: " + str(err)
    finally:
        conn.close()

    