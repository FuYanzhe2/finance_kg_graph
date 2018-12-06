# -*- coding: utf-8 -*-
from pymongo import MongoClient
from py2neo import Graph,Node,Relationship
import configparser
import pymysql
import codecs
import csv

class MysqlDb_config():

    def __init__(self):
        self.cfg_file = "../gol.cfg"
        self.conn = self.getMysqlConn()
        self.cur = self.conn.cursor()

    def _connect_mysql(self,cfg_file,db):
        conf = configparser.ConfigParser()
        conf.read(cfg_file)
        host = conf.get(db, "host")
        port = conf.get(db, "port")
        dbname = conf.get(db, "dbname")
        user_name = conf.get(db, "user_name")
        password = conf.get(db, "password")
        return host, port, dbname, user_name, password

    def getMysqlConn(self):
        host, port, dbname, user_name, password = self._connect_mysql(self.cfg_file,"mysql")
        conn = pymysql.connect(
            host=host,
            port=int(port),
            user=user_name,
            passwd=password,
            db=dbname,
            charset='utf8',
        )
        return conn
    def get_check_result(self,sql):
        self.cur.execute(sql)
        return self.cur.fetchall()

    def Insert_listData(self,TableName,table_index,dataset):
        """
        :param TableName:str类型
        :param table_index:列表 表头信息(%s)
        :param dataset:[Truple1,Truple2,Truple3......]
        """
        try:
            COLstr=''  #列的字段
            ColumnStyle=' TEXT'
            index_String ='('+",".join(table_index)+')'
            format_list = len(table_index)*['%s']
            template_sql = "insert into {} ".format(TableName)+index_String+' values '+"("+",".join(format_list)+")"
            for key in table_index:
                COLstr=COLstr+' '+key+ColumnStyle+','
            #推断表是否存在，存在运行try。不存在运行except新建表，再insert
            try:
                self.cur.execute("SELECT * FROM  %s"%(TableName))
                self.cur.executemany(template_sql,dataset)
            except pymysql.Error as e:
                self.cur.execute("CREATE TABLE %s (%s)"%(TableName,COLstr[:-1]))
                self.cur.executemany(template_sql,dataset)
            self.conn.commit()
        except pymysql.Error as e:

            print ("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def Insert_jsonData(self,TableName,dic):
        try:
            COLstr=''   #列的字段
            ROWstr=''  #行字段
            ColumnStyle=' TEXT(65536)'
            for key in dic.keys():
                if key=='分配方案':
                    continue
                COLstr=COLstr+' '+key+ColumnStyle+','
                ROWstr=ROWstr+'"{}"'.format(dic[key])+','
            COLstr = COLstr.replace("/","")
            COLstr = COLstr.replace("%","")
            #推断表是否存在，存在运行try。不存在运行except新建表，再insert
            try:
                self.cur.execute("SELECT * FROM  %s"%(TableName))
                self.cur.execute("INSERT INTO %s VALUES (%s)"%(TableName,ROWstr[:-1]))

            except pymysql.Error as e:
                self.cur.execute("CREATE TABLE %s (%s)"%(TableName,COLstr[:-1]))
                self.cur.execute("INSERT INTO %s VALUES (%s)"%(TableName,ROWstr[:-1]))

            self.conn.commit()
        except pymysql.Error as e:

            print ("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def read_sql_data(self,sql_text):
        self.cur.execute(sql_text)
        col_name = self.cur.description
        word_list = self.cur.fetchall()
        return col_name,word_list

    def read_mysql_to_csv(self,filename,sql_text):
        with codecs.open(filename=filename, mode='w', encoding='utf-8-sig') as f:
            write = csv.writer(f, dialect='excel')
            cols,results = self.read_sql_data(sql_text)
            line_0 = [col[0] for col in cols]
            write.writerow(line_0)
            write.writerows(results)

class MongoDb_config():
    def __init__(self):
        self.db = self.getMongoClient()

    def _get_mongodb_conn(self):
        conf = configparser.ConfigParser()
        conf.read("../gol.cfg")
        dsn = conf.get('mongodb','dsn')
        return dsn

    #获取mongodb访问连接
    def getMongoClient(self):
        dsn = self._get_mongodb_conn()
        client = MongoClient(dsn)
        db = client.finance
        return db

    #插入json或者[json,json]格式的数据
    def Insert_data(self,data,table_name):
        self.db[table_name].insert(data)

    def Get_data(self,table_name):
        data = self.db[table_name].find(no_cursor_timeout = True)
        return data

class Neo4j_config():
    def __init__(self,LOAD_DATA=True):
        self.cfg_file = "../gol.cfg"
        self.neo_graph = self.creat_neo4j_graph()
        if LOAD_DATA:
            self.neo_graph.delete_all()
            self.neo_graph.begin()

    def _get_neo4j_conn(self,db):
        conf = configparser.ConfigParser()
        conf.read(self.cfg_file)
        host = conf.get(db, "host")
        port = conf.get(db, "port")
        dbname = conf.get(db, "dbname")
        user_name = conf.get(db, "user_name")
        password = conf.get(db, "password")
        if db=="neo4j":
            bolt = conf.get(db, "bolt")
            return [host,port,bolt,dbname,user_name,password]
        else:
            return [host,port,dbname,user_name,password]

    def creat_neo4j_graph(self):
        host,port,bolt,dbname,user_name,password = self._get_neo4j_conn("neo4j")
        neo_graph = Graph(
            host = host,
            http_port =int(port),
            bolt_port=int(bolt),
            username=dbname,
            password=password
        )
        return neo_graph
