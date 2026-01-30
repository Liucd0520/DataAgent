
from eralchemy import render_er
from eralchemy.main import * 
# from eralchemy.cst import dot_crowfoot, dot_digraph
import pygraphviz as pgv
import json 
from bs4 import BeautifulSoup
from neo4j import GraphDatabase 

def generate_dot_from_uri(uri, title):
    tables, relationships = all_to_intermediary(uri,)
    tables, relationships = filter_resources(tables, relationships,)
    intermediary_to_output = get_output_mode('dummy.dot', 'auto')
    text = intermediary_to_output(tables, relationships, title).decode('utf-8')

    return text


def parse_attr(attr_dict):
    label = attr_dict['label']
    soup = BeautifulSoup(label, 'html.parser')
    table = soup.find('table')
    rows = table.find_all('tr')

    table_name = rows[0].find('font').text.strip()
    
    fields = []
    for row in rows[1:]:  # 跳过表头行
        td = row.find('td')
        port = td.get('port')  # 字段名（PORT 属性）
        content = td.text.strip()  # 字段完整内容
        
        # 解析字段名、类型、约束
        if '<u>' in str(td):  # 下划线表示主键
            is_primary = True
        else:
            is_primary = False
        
        # 提取字段名（第一个 FONT 标签内容）
        field_name = td.find('u').find('font').text.strip() if is_primary else td.find('font').text.strip()
        
        # 提取类型（[INTEGER] 部分）
        import re
        type_match = re.search(r'\[(.*?)\]', content)
        field_type = type_match.group(1) if type_match else ''
        
        # 提取约束（NOT NULL 等）
        constraints = []
        if 'NOT NULL' in content:
            constraints.append('NOT NULL')
        
        fields.append({
            'name': field_name,
            'type': field_type,
            'is_primary': is_primary,
            'constraints': constraints
        })
    
    return fields

def dot_to_json_pygraphviz(dot_text, json_file_path=None):
    """使用 pygraphviz 转换 DOT 到 JSON"""
    # 1. 读取 DOT 文件
    G = pgv.AGraph(dot_text)

    # 2. 提取图基本信息
    graph_type = "digraph" if G.is_directed() else "graph"
    graph_name = G.name
    graph_attrs = dict(G.graph_attr)  # 转换为字典

    # 3. 提取节点信息
    nodes = []
    for node_name in G.nodes():
        node_attrs = dict(G.get_node(node_name).attr)
        parsed_node_attr = parse_attr(node_attrs)
        nodes.append({
            "name": node_name,
            "attributes": parsed_node_attr
        })

    # 4. 提取边信息
    edges = []
    for src, dst in G.edges():
        edge_attrs = dict(G.get_edge(src, dst).attr)
        edges.append({
            "source": src,
            "target": dst,
            "attributes": edge_attrs
        })

    # 5. 构建 JSON 结构
    json_data = {
        "type": graph_type,
        "name": graph_name,
        "graph_attributes": graph_attrs,
        "nodes": nodes,
        "edges": edges
    }

    # 6. 输出 JSON 文件
    if json_file_path:
        with open(json_file_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        print(f"JSON 文件已生成：{json_file_path}")
    return json_data



def upload_to_neo4j(json_data, uri, username=None, password=None, task='update'):
    
    if username:
        driver = GraphDatabase.driver(uri, auth=(username, str(password)))
    else:
        driver = GraphDatabase.driver(uri)
    session = driver.session()
    if task == 'init':
        session.run("MATCH (n) DETACH DELETE n")
    
    # 数据库节点
    session.run(f"MERGE (n:Database {{name: '{json_data['name']}'}})")

    # 表节点
    for table in json_data['nodes']:
        session.run(f"MERGE (n:Table {{name: '{table['name']}', database: '{json_data['name']}'}})")
        session.run(f"MATCH (a:Database), (b:Table) WHERE a.name = '{json_data['name']}' AND b.name = '{table['name']}' AND b.database = '{json_data['name']}' MERGE (a)-[:TABLE]->(b) RETURN a,b")

        # 字段节点
        for column in table['attributes']:
            constraints = ', '.join(column['constraints'])
            print(f"DATABASE: {json_data['name']}, TABLE: {table['name']}, COLUMN: {column['name']}")
            session.run(f"MERGE (n:Column {{name: '{column['name']}', type: '{column['type']}', is_primary: '{column['is_primary']}', constraints: '{constraints}', database: '{json_data['name']}', table: '{table['name']}'}})")
            session.run(f"MATCH (a:Table), (b:Column) WHERE a.name = '{table['name']}' AND b.name = '{column['name']}' AND a.database = '{json_data['name']}' AND b.database = '{json_data['name']}' AND b.table = '{table['name']}' MERGE (a)-[:COLUMN]->(b) RETURN a,b")
    
    # 额外关系
    for relation in json_data['edges']:
  
        print(f"{relation['source']}.{relation['attributes']['tailport']} TO {relation['target']}.{relation['attributes']['headport']}")
        session.run(f"MATCH (a:Column), (b:Column) WHERE b.name = '{relation['attributes']['tailport']}' AND b.table = '{relation['source']}' AND a.name = '{relation['attributes']['headport']}' AND a.table = '{relation['target']}' MERGE (a)-[:IS]->(b) RETURN a,b")


if __name__ == '__main__':
    from urllib.parse import quote_plus
    db_uri = f"mysql+mysqlconnector://ai_test:{quote_plus('Netcare@13579')}@172.31.26.206:3306/netcaredb_ai"
    neo4j_uri =  f'bolt://172.31.24.111:7689'
    dot_text = generate_dot_from_uri(db_uri, '刘长东')
    json_data = dot_to_json_pygraphviz(dot_text, 'a.json')
    json_data['name'] = '12345' # database节点的name
    # upload_to_neo4j(json_data, neo4j_uri, username='neo4j', password='12345678', task='init')

    # 获取neo4j的schema可以用langchain_neo4j这个库 Neo4jGraph类对应的schema方法