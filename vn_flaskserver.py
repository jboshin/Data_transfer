from flask import Flask, request
import  pymysql
import json
app = Flask(__name__)

# 連接Alarm資料庫
def insert_Alarm(Id, FacName, NodeID, NodeName, Datetime, CurrentAVG, MaxA,VoltageAVG, MaxV, MinV):
    db_settings = {
    "host": "127.0.0.1",
    "user": "bs",
    "password": <資料庫密碼>,
    "db": "BS_DB",}
    try:
        # 建立Connection物件
        conn = pymysql.connect(**db_settings)
        # 建立Cursor物件
        with conn.cursor() as cursor:
            #資料表相關操作
            command = "INSERT IGNORE INTO TB_Alarm (Id, FacName, NodeID, NodeName, Datetime, CurrentAVG, MaxA, VoltageAVG, MaxV, MinV) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(command, (Id, FacName, NodeID, NodeName, Datetime, CurrentAVG, MaxA,VoltageAVG, MaxV, MinV))
            # 儲存變更
            conn.commit()
    except Exception as ex:
        print(ex)
        
# 連接Info資料庫
def insert_Info(Id, FacID, FacName, NodeID, NodeName, Datetime, CurrentAVG, VoltageAVG, PowerTotal, PowerFactorTotal, ActiveEnergyDelivered, CurrentA, CurrentB, CurrentC, VoltageAB, VoltageBC, VoltageCA, THDIA, THDIB, THDIC, THDVAB, THDVBC, THDVCA, THDVLL, QTot, STot):
    db_settings = {
    "host": "127.0.0.1",
    "user": "bs",
    "password": "C217_bs",
    "db": "BS_DB",}
    try:
        # 建立Connection物件
        conn = pymysql.connect(**db_settings)
        # 建立Cursor物件
        with conn.cursor() as cursor:
            #資料表相關操作
            command = "INSERT IGNORE INTO TB_Info (Id, FacID, FacName, NodeID, NodeName, Datetime, CurrentAVG, VoltageAVG, PowerTotal, PowerFactorTotal, ActiveEnergyDelivered, CurrentA, CurrentB, CurrentC, VoltageAB, VoltageBC, VoltageCA, THDIA, THDIB, THDIC, THDVAB, THDVBC, THDVCA, THDVLL, QTot, STot) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(command, (Id, FacID, FacName, NodeID, NodeName, Datetime, CurrentAVG, VoltageAVG, PowerTotal, PowerFactorTotal, ActiveEnergyDelivered, CurrentA, CurrentB, CurrentC, VoltageAB, VoltageBC, VoltageCA, THDIA, THDIB, THDIC, THDVAB, THDVBC, THDVCA, THDVLL, QTot, STot))
            # 儲存變更
            conn.commit()
    except Exception as ex:
        print(ex)
        
# 接收alarm資料
@app.route('/alarm', methods=['POST'])
def alarm_register():
    # print (request.headers)
    # print (request.form)
    input_data = request.get_data()
    dict_data = json.loads(input_data)
    Id = str(dict_data["Id"])
    FacName = str(dict_data["FacName"])
    NodeID = str(dict_data["NodeID"])
    NodeName = str(dict_data["NodeName"])
    Datetime = str(dict_data["Datetime"])
    CurrentAVG = str(dict_data["CurrentAVG"])
    MaxA = str(dict_data["MaxA"])
    VoltageAVG = str(dict_data["VoltageAVG"])
    MaxV = str(dict_data["MaxV"])
    MinV = str(dict_data["MinV"])
    # 將資料傳送進DB
    insert_Alarm(Id, FacName, NodeID, NodeName, Datetime, CurrentAVG, MaxA,VoltageAVG, MaxV, MinV)
    return Id
  
# 接收info資料
@app.route('/info', methods=['POST'])
def info_register():
    # print (request.headers)
    # print (request.form)
    input_data = request.get_data()
    dict_data = json.loads(input_data)
    Id = str(dict_data["Id"])
    FacID = str(dict_data["FacID"])
    FacName = str(dict_data["FacName"])
    NodeID = str(dict_data["NodeID"])
    NodeName = str(dict_data["NodeName"])
    Datetime = str(dict_data["Datetime"])
    CurrentAVG = str(dict_data["CurrentAVG"])
    VoltageAVG = str(dict_data["VoltageAVG"])
    PowerTotal = str(dict_data["PowerTotal"])
    PowerFactorTotal = str(dict_data["PowerFactorTotal"])
    ActiveEnergyDelivered = str(dict_data["ActiveEnergyDelivered"])
    CurrentA = str(dict_data["CurrentA"])
    CurrentB = str(dict_data["CurrentB"])
    CurrentC = str(dict_data["CurrentC"])
    VoltageAB = str(dict_data["VoltageAB"])
    VoltageBC = str(dict_data["VoltageBC"])
    VoltageCA = str(dict_data["VoltageCA"])
    THDIA = str(dict_data["THDIA"])
    THDIB = str(dict_data["THDIB"])
    THDIC = str(dict_data["THDIC"])
    THDVAB = str(dict_data["THDVAB"])
    THDVBC = str(dict_data["THDVBC"])
    THDVCA = str(dict_data["THDVCA"])
    THDVLL = str(dict_data["THDVLL"])
    QTot = str(dict_data["QTot"])
    STot = str(dict_data["STot"])
    # 將資料傳送進DB
    insert_Info(Id, FacID, FacName, NodeID, NodeName, Datetime, CurrentAVG, VoltageAVG, PowerTotal, PowerFactorTotal, ActiveEnergyDelivered, CurrentA, CurrentB, CurrentC, VoltageAB, VoltageBC, VoltageCA, THDIA, THDIB, THDIC, THDVAB, THDVBC, THDVCA, THDVLL, QTot, STot)
    return Id
  
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8732 , debug=True)
