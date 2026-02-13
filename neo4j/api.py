from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import subprocess
from typing import Optional, List

from neo4j import GraphDatabase
import mysql.connector
from mysql.connector import Error

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI()

def decrypt(b64str):
    import base64
    return base64.b64decode(b64str).decode('utf-8')

def encrypt(original):
    import base64
    return base64.b64encode(original.encode('utf-8')).decode('utf-8')



class MySQLConnection:
    def __init__(self):
        try:
            self.connection = mysql.connector.connect(
                host="172.31.24.112",
                port=3307,
                user="root",
                password="my-secret-pw",
                database="zhirong_db"
            )
            if self.connection.is_connected():
                # 验证连接成功并输出服务器信息
                db_version = self.connection.server_info
                print(f"✅ 成功连接MySQL服务器，版本：{db_version}")
                
                # 创建游标
                self.cursor = self.connection.cursor(dictionary=True)
        except Error as e:
            # 捕获并清晰输出错误信息
            print(f"❌ 数据库连接失败：{e}")
    
    def init_db(self, id, sourceid, no=1):
        from datetime import datetime
        current_time = datetime.now()
        formatted_current_time = current_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        if no == 1:
            uri = '172.31.24.111'
            port = '7476'
            bolt = '7689'
        elif no == 2:
            uri = '172.31.24.111'
            port = '7475'
            bolt = '7688'
        self.cursor.execute(f"INSERT INTO datasource_neo (id, datasourceId, host, port, bolt, username, passwordEncrypted, status, createdAt, updatedAt) VALUES ('{id}', '{sourceid}', '{uri}', '{port}', '{bolt}', 'neo4j', '{encrypt('12345678')}', 'INIT', '{formatted_current_time}', '{formatted_current_time}')")
            

    def change_status(self, id, status = 'OK'):
        from datetime import datetime
        current_time = datetime.now()
        formatted_current_time = current_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        self.cursor.execute(f"UPDATE datasource_neo SET `status` = '{status}', `updatedAt` = '{formatted_current_time}' WHERE id = '{id}'")

    def get_db_id(self, sourceid):
        self.cursor.execute(f"SELECT id from datasource_neo WHERE datasourceId = '{sourceid}'")
        result = self.cursor.fetchall()
        return result[0]['id']
    
    def get_source_info(self, sourceid):
        self.cursor.execute(f"SELECT host, port, username, passwordEncrypted, database from datasources WHERE id = '{sourceid}'")
        result = self.cursor.fetchall()
        return result[0]['host'], result[0]['port'], result[0]['username'], result[0]['password'], result[0]['database'] 
    
    def get_tables(self, sourceid):
        self.cursor.execute(f"SELECT tableName from datasource_tables WHERE datasourceId = '{sourceid}' AND selected = 1")
        result = self.cursor.fetchall()
        tables = []
        for item in result:
            tables.append(item['tableName'])
        return tables
    
    def get_neo(self, sourceid):
        self.cursor.execute(f"SELECT port from datasource_neo WHERE datasourceiId = '{sourceid}'")
        result = self.cursor.fetchall()
        if int(result[0]['port']) == 7476:
            return 2
        elif int(result[0]['port']) == 7475:
            return 1


class Neo4jConnection:
    def __init__(self, id, mysql: MySQLConnection):
        cursor = mysql.cursor
        cursor.execute(f"SELECT * FROM datasource_neo WHERE datasourceId='{id}' AND status='OK'")
        result = cursor.fetchall()[0]
        if result['username']:
            driver = GraphDatabase.driver(f"bolt://{result['host']}:{result['bolt']}", auth=(result['username'], decrypt(result['passwordEncrypted'])))
        else:
            driver = GraphDatabase.driver(f"bolt://{result['host']}:{result['bolt']}")
        self.session = driver.session()

            

class CreateRequest(BaseModel):
    id: str

class DelRequest(BaseModel):
    id: str

class ExecRequest(BaseModel):
    id: str
    commands: List

class GetRequest(BaseModel):
    id: str

'''def delayed_change_status(id: str):
    import time
    time.sleep(30)
    conn = MySQLConnection()
    conn.change_status(id, 'OK')'''

def background_create_db(id: str, sourceid: str, conn: MySQLConnection, no=1):
    from dot_to_json import TransferConfig, generate
    include_tables = conn.get_tables(sourceid)
    uri, port, username, password, database = conn.get_source_info(sourceid)
    if no == 1:
        neo_port = 7689
    elif no == 2:
        neo_port = 7688
    transferconfig = {
        "database": {
            "uri": uri,
            "port": int(port),
            "username": username,
            "password": decrypt(password),
            "database": database
        },
        "ER": {
            "json_file_path": None,
            "include_tables": include_tables,
            "include_columns": None,
            "exclude_tables": None,
            "exclude_columns": None,
            "schema": None,
            "title": database
        },
        "graph": {
            "mode": "init",
            "uri": "172.31.24.111",
            "port": neo_port,
            "username": "neo4j",
            "password": "12345678"
        },
        "filter": {
            "mode": "high",
            "output_file": None,
            "filtered_output_file": None,
            "coverage_threshold": 0.85,
            "max_null_ratio": 0.5,
            "max_cardinality_ratio": 1.2,
            "min_name_similarity": 0.3
        }
    }
    config = TransferConfig(transferconfig)
    generate(config)
    conn.change_status(id, 'OK')


@app.post("/create")
async def create_db(request: CreateRequest, background_tasks: BackgroundTasks):
    try:
        conn = MySQLConnection()
        neo4j_conn = Neo4jConnection(request.id, conn)
        session = neo4j_conn.session

        import uuid
        datasourceId = request.id
        id = uuid.uuid4()
        no = conn.get_neo(datasourceId)
        conn.init_db(id, datasourceId, no)

        # 添加后台任务，30秒后执行 change_status
        background_tasks.add_task(background_create_db, str(id), str(datasourceId), conn, no)

        return {
            "message": "",
            "status": "OK"
        }
    except Exception as e:
        return {
            "message": str(e),
            "status": "error"
        }
    
@app.post("/del")
async def del_db(request: DelRequest):
    try:
        conn = MySQLConnection()
        neo4j_conn = Neo4jConnection(request.id, conn)
        session = neo4j_conn.session
        session.run("MATCH (n) DETACH DELETE n")
        return {
            "message": "",
            "status": "OK"
        }
    except Exception as e:
        return {
            "message": str(e),
            "status": "error"
        }


@app.post("/get")
async def get_endpoint(request: GetRequest):
    """
    获取接口
    节点: MATCH (n) RETURN n AS Node, labels(n) AS Label
    关系: MATCH (a)-[r]->(b) RETURN a AS StartNode, labels(a) AS StartLabel, r AS Relationship, type(r) AS TypeRelationship, b AS Endnode, labels(b) As EndLabel
    """
    logger.info(f"收到查询请求: {request}")
    try:
        conn = MySQLConnection()
        neo4j_conn = Neo4jConnection(request.id, conn)
        session = neo4j_conn.session

        node_result = session.run("MATCH (n) RETURN n AS Node, labels(n) AS Label")
        node_data = node_result.data()
        rel_result = session.run("MATCH (a)-[r]->(b) RETURN a AS StartNode, labels(a) AS StartLabel, r AS Relationship, type(r) AS TypeRelationship, b AS Endnode, labels(b) As EndLabel")
        rel_data = rel_result.data()
        
        logger.info(f"完成查询请求: {request}")
        return {
            "message": "",
            "status": "OK",
            "node": node_data,
            "rel": rel_data
        }
    except Exception as e:
        logger.error(f"处理问题时出错: {str(e)}")
        # print(e)
        '''return {
            "message": e._message,
            "status": "error",
            "node": [],
            "rel": []
        }'''
        return {
            "message": str(e),
            "status": "error",
            "node": [],
            "rel": []
        }


@app.post("/exec")
async def exec_endpoint(request: ExecRequest):
    """
    EXEC 接口示例 - 执行命令
    """
    logger.info(f"收到执行请求: {request}")
    try:
        conn = MySQLConnection()
        neo4j_conn = Neo4jConnection(request.id, conn)
        session = neo4j_conn.session
        commands = request.commands
        for cmd in commands:
            startnode = cmd['relation']['StartNode']['id']
            startlabel = cmd['relation']['StartLabel'][0]
            endnode = cmd['relation']['Endnode']['id']
            endlabel = cmd['relation']['EndLabel'][0]
            relationlabel = cmd['relation']['TypeRelationship']
            if cmd['type'] == 'create':
                session.run(f"MATCH (a:{startlabel}), (b:{endlabel}) WHERE a.id='{startnode}' AND b.id='{endnode}' CREATE (a)-[:{relationlabel}]->(b) RETURN a,b")
            elif cmd['type'] == 'update':
                oldrelationlabel = cmd['relation']['TypeRelationshipOld']
                session.run(f"MATCH (a:{startlabel})-[r:{oldrelationlabel}]->(b:{endlabel}) WHERE a.id='{startnode}' AND b.id='{endnode}' DELETE r RETURN a,b")
                session.run(f"MATCH (a:{startlabel}), (b:{endlabel}) WHERE a.id='{startnode}' AND b.id='{endnode}' CREATE (a)-[:{relationlabel}]->(b) RETURN a,b")
            elif cmd['type'] == 'delete':
                session.run(f"MATCH (a:{startlabel})-[r:{relationlabel}]->(b:{endlabel}) WHERE a.id='{startnode}' AND b.id='{endnode}' DELETE r RETURN a,b")
        
        logger.info(f"完成执行请求: {request}")
        return {
            "message": "",
            "status": "OK",
        }
    except Exception as e:
        logger.error(f"处理问题时出错: {str(e)}")
        # print(e)
        return {
            "message": str(e),
            "status": "error",
        }


if __name__ == "__main__":
    import uvicorn
    # conn = MySQLConnection()
    uvicorn.run(app, host="0.0.0.0", port=7850)
    # cursor = MySQLConnection()