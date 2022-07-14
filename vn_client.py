import pyodbc
import requests
import json
from datetime import datetime
import decimal
import configparser
import sqlite3
import os

# 調整JSON格式
class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj)
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S') 
        return super(ExtendJSONEncoder, self).default(obj)
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn

# 取得最後ID
def get_last_ID():
    try:
        # 取得Alarm最後ID
        db_table = config['database']['table_Alarm']
        command = "select top 1 * from %s order by Id DESC" %(db_table)
        cur.execute(command)
        # 擷取ID
        for row in cur:
            new_dict = {}
            new_dict['Id'] = row[0]
        Alarm_last_ID = new_dict.get('Id')
        # 取得Info最後ID
        db_table = config['database']['table_Info']
        command = "select top 1 * from %s order by Id DESC" %(db_table)
        cur.execute(command)
        # 擷取ID
        for row in cur:
            new_dict = {}
            new_dict['Id'] = row[0]
        Info_last_ID = new_dict.get('Id')
        
        # 檢查last_ID.db是否存在 若不存在創建DB
        if os.path.isfile('last_ID.db'):
            pass
        else:
            con = sqlite3.connect('last_ID.db')
            c = con.cursor()
            c.execute('''CREATE TABLE last_ID
            (Alarm_ID INT NOT NULL, Info_ID INT NOT NULL);''')
            con.commit()
            con.close()
        # 填入最後ID
        con = sqlite3.connect('last_ID.db')
        c = con.cursor()
        c.execute("DELETE FROM last_ID;")
        c.execute("INSERT INTO last_ID (Alarm_ID, Info_ID) VALUES (%i, %i);"%(Alarm_last_ID, Info_last_ID))
        con.commit()
        con.close()
    except Error as e:
        print(e)

# 傳送Alarm資料至Server
def post_Alarm(last_ID):
    try:
        # 資料庫指令
        db_table = config['database']['table_Alarm']
        command ="SELECT * FROM %s where ID > %s" %(db_table, last_ID)
        cur.execute(command)
        # 擷取資料
        for row in cur:
            new_dict = {}
            new_dict['Id'] = row[0]
            new_dict['FacName'] = row[1]
            new_dict['NodeID'] = row[2]
            new_dict['NodeName'] = row[3]
            new_dict['Datetime'] = row[4]
            new_dict['CurrentAVG'] = row[5]
            new_dict['MaxA'] = row[6]
            new_dict['VoltageAVG'] = row[7]
            new_dict['MaxV'] = row[8]
            new_dict['MinV'] = row[9]
                  # 轉換成JSON
            # print("orginal:",new_dict)
            data = json.dumps(new_dict, cls=ComplexEncoder)
            # print("json:",data)
            print("POST Alarm:",data)
            # post資料至server
            uclip = config['uclip']['ip']
            r = requests.post("http://%s/alarm" %(uclip), data=data)
    except Error as e:
        print(e)

# 傳送Info資料至Server
def post_Info(last_ID):
    try:
        # 資料庫指令
        db_table = config['database']['table_Info']
        command ="SELECT * FROM %s where ID > %s" %(db_table, last_ID)
        cur.execute(command)
        # 擷取資料
        for row in cur:
            new_dict = {}
            new_dict['Id'] = row[0]
            new_dict['FacID'] = row[1]
            new_dict['FacName'] = row[2]
            new_dict['NodeID'] = row[3]
            new_dict['NodeName'] = row[4]
            new_dict['Datetime'] = row[5]
            new_dict['CurrentAVG'] = row[6]
            new_dict['VoltageAVG'] = row[7]
            new_dict['PowerTotal'] = row[8]
            new_dict['PowerFactorTotal'] = row[9]
            new_dict['ActiveEnergyDelivered'] = row[10]
            new_dict['CurrentA'] = row[11]
            new_dict['CurrentB'] = row[12]
            new_dict['CurrentC'] = row[13]
            new_dict['VoltageAB'] = row[14]
            new_dict['VoltageBC'] = row[15]
            new_dict['VoltageCA'] = row[16]
            new_dict['THDIA'] = row[17]
            new_dict['THDIB'] = row[18]
            new_dict['THDIC'] = row[19]
            new_dict['THDVAB'] = row[20]
            new_dict['THDVBC'] = row[21]
            new_dict['THDVCA'] = row[22]
            new_dict['THDVLL'] = row[23]
            new_dict['QTot'] = row[24]
            new_dict['STot'] = row[25]
                  # 轉換成JSON
            # print("orginal:",new_dict)
            data = json.dumps(new_dict, cls=ComplexEncoder)
            # print("json:",data)
            print("POST Info:",data)
            # post資料至server
            uclip = config['uclip']['ip']
            r = requests.post("http://%s/info" %(uclip), data=data)
    except Error as e:
        print(e)

# 使用config
config = configparser.ConfigParser()
config.read('config.ini',encoding="utf-8")

# 連接資料庫
server = config['database']['server']
db = config['database']['db']
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=%s'%(server, db))
cur = conn.cursor()

# 檢查last_ID.db是否存在
if os.path.isfile('last_ID.db'):
    # 讀取前一筆最後ID
    database = r"last_ID.db"
    id_dict = {}
    con = create_connection(database)
    c = con.cursor()
    c.execute("SELECT * FROM last_ID")
    # 擷取資料
    for row in c:
        id_dict['Alarm_last_ID'] = row[0]
        id_dict['Info_last_ID'] = row[1]
    Alarm_last_ID = id_dict.get('Alarm_last_ID')
    Info_last_ID = id_dict.get('Info_last_ID')
    # 傳送資料
    post_Alarm(Alarm_last_ID)
    post_Info(Info_last_ID)
    # 執行完畢後記錄最後ID
    get_last_ID()
else:
    # 傳送資料
    post_Alarm(0)
    post_Info(0)
    # 執行完畢後記錄最後ID
    get_last_ID()
