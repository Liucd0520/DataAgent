import json
import pygraphviz as pgv
from eralchemy.main import *

import configparser
from argparse import ArgumentParser

from typing import List, Dict, Set, Tuple
from collections import defaultdict
from itertools import combinations

from bs4 import BeautifulSoup

from implicit_fk_discovery import ImplicitFKDiscoverer

from filter_implicit_fks import filter_implicit_foreign_keys, advanced_filter, categorize_relationships
import time

def convert_str_out(item):
    try:
        item = eval(item.strip())
        return item
    except:
        item = item.strip()
    try:
        return int(item)
    except:
        pass

    try:
        return float(item)
    except:
        pass

    if item.lower() in ['true', 'false']:
        return bool(item)
    else:
        return item

def convert_dict_values(d):
    mapped_items = map(lambda item:(item[0], convert_str_out(item[1])), d.items())
    return dict(mapped_items)

class TransferConfig:
    def __init__(self, config_path):
        if isinstance(config_path, str):
            self.config = configparser.ConfigParser()
            self.config.read(config_path, encoding='utf-8')
            self.database = convert_dict_values(dict(self.config['database']))
            self.ER = convert_dict_values(dict(self.config['ER']))
            self.graph = convert_dict_values(dict(self.config['graph']))
            self.filter = convert_dict_values(dict(self.config['filter']))
        elif isinstance(config_path, dict):
            self.database = config_path['database']
            self.ER = config_path['ER']
            self.graph = config_path['graph']
            self.filter = config_path['filter']

    def get(self, section):
        return getattr(self, section)

def generate_dot_from_uri(uri, include_tables=None, include_columns=None, exclude_tables=None, exclude_columns=None, schema=None, title=None):
    tables, relationships = all_to_intermediary(uri, schema=schema)
    tables, relationships = filter_resources(tables, relationships, include_tables=include_tables, include_columns=include_columns, exclude_tables=exclude_tables, exclude_columns=exclude_columns)
    intermediary_to_output = get_output_mode('dummy.dot', 'auto')
    text = intermediary_to_output(tables, relationships, title).decode('utf-8')

    return text

def discover_relationship(host, port, username, password, database, coverage_threshold=0.85, max_null_ratio=0.5, output_file=None, include_tables=None, exclude_tables=None, include_columns=None, exclude_columns=None):
    # return []
    discoverer = ImplicitFKDiscoverer(
        host=host,
        port=port,
        user=username,
        password=password,
        database=database
    )
    relationships = discoverer.discover_implicit_foreign_keys(
        coverage_threshold=coverage_threshold,
        max_null_ratio=max_null_ratio,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
        include_columns=include_columns,
        exclude_columns=exclude_columns
    )

    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(relationships, f, indent=2, ensure_ascii=False)

    return relationships

def filter_relationship(relationships, mode='high', min_coverage=0.85, max_null_ratio=0.5, max_cardinality_ratio=1.2, min_name_similarity=0.3, filtered_output_file=None):
    filtered_basic = filter_implicit_foreign_keys(
        relationships=relationships,
        min_coverage=min_coverage,
        max_null_ratio=max_null_ratio,
        max_cardinality_ratio=max_cardinality_ratio,
        min_name_similarity=min_name_similarity
    )

    filtered_out = filtered_basic

    if mode != 'basic':
        filtered_advanced = advanced_filter(relationships)
        filtered_out = filtered_advanced

        if mode in ['high']:
            categories = categorize_relationships(filtered_advanced)

            filtered_out = categories['high_quality']

    if filtered_output_file:
        with open(filtered_output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_out, f, indent=2, ensure_ascii=False)

    return filtered_out

def find_shared_reference_clusters(relationships, json_data, min_cluster_size=2):
    reference_groups = defaultdict(dict)
    labels = {}
    local_relationships = remove_duplicate_edges(json_data['edges'])
    # local_groups = []
    for edge in local_relationships:
        n = len(reference_groups.items())
        pk_table = (edge['target'], edge['attributes']['headport'])
        fk_table = (edge['source'], edge['attributes']['tailport'])
        if f"{edge['target']}.{edge['attributes']['headport']}" not in labels.keys():
            labels[f"{edge['target']}.{edge['attributes']['headport']}"] = 'explicit'
        if f"{edge['source']}.{edge['attributes']['tailport']}" not in labels.keys():
            labels[f"{edge['source']}.{edge['attributes']['tailport']}"] = 'explicit'

        if len(reference_groups.items()) == 0:
            action = ('new', f'cluster{n}', [fk_table, pk_table])
            # reference_groups[f'cluster{n}'] = []
            # reference_groups[f'cluster{n}'].append(fk_table)
            # reference_groups[f'cluster{n}'].append(pk_table)
        else:
            for k, v in reference_groups.items():
                if pk_table in v:
                    action = ('add', k, fk_table)
                    break
                    # reference_groups[k].append(fk_table)
                elif fk_table in v:
                    action = ('add', k, pk_table)
                    break
                    # reference_groups[k].append(pk_table)
                else:
                    action = ('new', f'cluster{n}', [fk_table, pk_table])
                    # reference_groups[f'cluster{n}'] = []
                    # reference_groups[f'cluster{n}'].append(fk_table)
                    # reference_groups[f'cluster{n}'].append(pk_table)
        
        if action[0] == 'add':
            if action[2] not in reference_groups[action[1]]:
                reference_groups[action[1]].append(action[2])
        elif action[0] == 'new':
            reference_groups[action[1]] = []
            reference_groups[action[1]].append(action[2][0])
            reference_groups[action[1]].append(action[2][1])

    for rel in relationships:
        n = len(reference_groups.items())
        pk_table = (rel['pk_table'], rel['pk_column'])
        fk_table = (rel['fk_table'], rel['fk_column'])
        if f"{rel['pk_table']}.{rel['pk_column']}" not in labels.keys():
            labels[f"{rel['pk_table']}.{rel['pk_column']}"] = 'implicit'
        if f"{rel['fk_table']}.{rel['fk_column']}" not in labels.keys():
            labels[f"{rel['fk_table']}.{rel['fk_column']}"] = 'implicit'
        if len(reference_groups.items()) == 0:
            action = ('new', f'cluster{n}', [fk_table, pk_table])
            # reference_groups[f'cluster{n}'] = []
            # reference_groups[f'cluster{n}'].append(fk_table)
            # reference_groups[f'cluster{n}'].append(pk_table)
        else:
            for k, v in reference_groups.items():
                if pk_table in v:
                    action = ('add', k, fk_table)
                    break
                    # reference_groups[k].append(fk_table)
                elif fk_table in v:
                    action = ('add', k, pk_table)
                    break
                    # reference_groups[k].append(pk_table)
                else:
                    action = ('new', f'cluster{n}', [fk_table, pk_table])
                    # reference_groups[f'cluster{n}'] = []
                    # reference_groups[f'cluster{n}'].append(fk_table)
                    # reference_groups[f'cluster{n}'].append(pk_table)
        if action[0] == 'add':
            if action[2] not in reference_groups[action[1]]:
                reference_groups[action[1]].append(action[2])
        elif action[0] == 'new':
            reference_groups[action[1]] = []
            reference_groups[action[1]].append(action[2][0])
            reference_groups[action[1]].append(action[2][1])

    clusters = [reference_groups, labels]
    with open('/data/liyiru/mysql-graph/pre_cluster.json', 'w', encoding='utf-8') as pre_f:
        json.dump(clusters, pre_f, indent=2, ensure_ascii=False)
    '''print("GENERATED PRE CLUSTER")
    time.sleep(1000)'''

    # for k, v in reference_groups.items()

    # 3. 构建聚类结果
    '''clusters = []
    for ref_key, tables in reference_groups.items():
        if len(tables) >= min_cluster_size:
            clusters.append({
                'referenced_table': ref_key[0],
                'referenced_column': ref_key[1],
                'referencing_tables': tables,
                'cluster_size': len(tables),
                'potential_connections': len(tables) * (len(tables) - 1) // 2
            })'''

    '''clusters.sort(key=lambda x: x['cluster_size'], reverse=True)

    with open("/data/liyiru/mysql-graph/cluster.json", 'w', encoding='utf-8') as f2:
        json.dump(clusters, f2, indent=2, ensure_ascii=False)'''

    return clusters

def fix_transitive(relationships, json_data):
    clusters = find_shared_reference_clusters(
        relationships,
        json_data,
        min_cluster_size=2
    )

    return clusters

def remove_duplicate_edges(edges):
    """根据 source 和 target 去重"""
    seen = set()
    unique_edges = []
    for edge in edges:
        if edge['source'] == edge['target']:
            pass
        else:
            new_item = {"fk_table": edge['source'], "fk_column": edge['attributes']['tailport'], "pk_table": ['target'], "pk_column": edge['attributes']['headport']}
            # 创建唯一标识（根据关键字段）
            key = (edge['source'], edge['target'], edge['attributes']['tailport'], edge['attributes']['headport'],)
            if key not in seen:
                seen.add(key)
                unique_edges.append(edge)
    return unique_edges
    

# def generate_er(mysql_uri, mysql_port, mysql_username, mysql_password, mysql_database, neo4j_uri, neo4j_port, neo4j_username=None, neo4j_password=None, mode='init', json_file_path=None, include_tables=None, include_columns=None, exclude_tables=None, exclude_columns=None, schema=None, title=None):

def generate(config: TransferConfig):
    database_info = config.get('database')
    ER_info = config.get('ER')
    graph_info = config.get('graph')
    filter_info = config.get('filter')
    db_uri = f'mysql+mysqlconnector://{database_info["username"].replace("@", "%40")}:{database_info["password"].replace("@", "%40")}@{database_info["uri"]}:{database_info["port"]}/{database_info["database"]}'
    # db_uri = f'mysql+mysqlconnector://{mysql_username}:{mysql_password.replace("@", "%40")}@{mysql_uri}:{mysql_port}/{mysql_database}' # 组合mysql数据库uri
    print("GENERATING DOT FILE")
    dot_text = generate_dot_from_uri(db_uri, ER_info['include_tables'], ER_info['include_columns'], ER_info['exclude_tables'], ER_info['exclude_columns'], ER_info['schema'], ER_info['title'])

    # dot_text = generate_dot_from_uri(db_uri, include_tables, include_columns, exclude_tables, exclude_columns, schema, title) # 生成dot格式er图
    print("GENERATING ER GRAPH")
    json_data = dot_to_json_pygraphviz(dot_text, json_file_path=ER_info['json_file_path'])
    # json_data = dot_to_json_pygraphviz(dot_text, json_file_path=json_file_path) # 生成解析完成的json数据
    json_data['name'] = database_info['database']
    neo4j_uri = f'bolt://{graph_info["uri"]}:{graph_info["port"]}'

    # time.sleep(1000)
    
    # neo4j_uri = f'bolt://{neo4j_uri}:{neo4j_port}' #组合neo4j数据库uri

    print("UPLOADING GRAPH NODES")
    upload_to_neo4j(json_data, neo4j_uri, graph_info['username'], graph_info['password'], graph_info['mode'])
    # upload_to_neo4j(json_data, neo4j_uri, neo4j_username, neo4j_password, mode) # 上传知识图谱

    print("ANALYZING RELATIONSHIPS")
    relationships = discover_relationship(
        database_info['uri'], database_info['port'], database_info['username'], database_info['password'], database_info['database'],
        filter_info['coverage_threshold'], filter_info['max_null_ratio'], filter_info['output_file'],
        include_tables=ER_info['include_tables'],
        exclude_tables=ER_info['exclude_tables'],
        include_columns=ER_info['include_columns'],
        exclude_columns=ER_info['exclude_columns']
    ) # 寻找隐式关联

    print("FILTERING RELATIONSHIPS")
    filtered = filter_relationship(relationships=relationships, mode=filter_info['mode'], min_coverage=filter_info['coverage_threshold'], max_null_ratio=filter_info['max_null_ratio'], max_cardinality_ratio=filter_info['max_null_ratio'], min_name_similarity=filter_info['min_name_similarity'], filtered_output_file=filter_info['filtered_output_file']) # 过滤合适质量的关联

    print("CLUSTERING")
    # print("THIS IS FILTERED:", filtered)
    # time.sleep(1000)
    clusters = fix_transitive(filtered, json_data) # 聚合相同关系
    uploads = form_relationships_from_clusters(clusters) # 聚类

    # print("GENERATE UPLOADS")
    # time.sleep(1000)

    print("UPLOADING RELATIONSHIPS")
    # print(uploads)
    # print(uploads[0])
    upload_relations_to_neo4j(uploads, neo4j_uri, graph_info['username'], graph_info['password']) # 上传大模型生成的关联
    





def dot_to_json_pygraphviz(dot_file_path, json_file_path=None):
    """使用 pygraphviz 转换 DOT 到 JSON"""
    # 1. 读取 DOT 文件
    G = pgv.AGraph(dot_file_path)

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
    # if json_file_path:
    if True:
        with open('json_data.json', 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        print(f"JSON 文件已生成：{json_file_path}")
    return json_data

def upload_to_neo4j(json_data, uri, username=None, password=None, task='update'):
    from neo4j import GraphDatabase
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
    '''for relation in json_data['edges']:
  
        print(f"{relation['source']}.{relation['attributes']['headport']} TO {relation['target']}.{relation['attributes']['tailport']}")
        session.run(f"MATCH (a:Column), (b:Column) WHERE b.name = '{relation['attributes']['headport']}' AND b.table = '{relation['source']}' AND a.name = '{relation['attributes']['tailport']}' AND a.table = '{relation['target']}' MERGE (a)-[:IS]->(b) RETURN a,b")'''

'''def form_relationships_from_clusters(clusters):
    uploads = []
    for cluster in clusters:
        each = []
        each.append((cluster['referenced_table'], cluster['referenced_column']))
        for item in cluster['referencing_tables']:
            each.append((item['table'], item['column']))

        uploads.append(each)

    with open('uploads.json', 'w', encoding='utf-8') as f:
        json.dump(uploads, f, indent=2, ensure_ascii=False)

    return uploads'''

def form_relationships_from_clusters(clusters):
    uploads = []
    for key, cluster in clusters[0].items():
        for i in range(0, len(cluster)):
            for j in range(i+1, len(cluster)):
                column0 = f"{cluster[i][0]}.{cluster[i][1]}"
                column1 = f"{cluster[j][0]}.{cluster[j][1]}"
                if clusters[1][column0] == "explicit" and clusters[1][column1] == "explicit":
                    uploads.append({
                        "source_table": cluster[i][0],
                        "source_column": cluster[i][1],
                        "target_table": cluster[j][0],
                        "target_column": cluster[j][1],
                        "relation": "IS"
                    })
                else:
                    uploads.append({
                        "source_table": cluster[i][0],
                        "source_column": cluster[i][1],
                        "target_table": cluster[j][0],
                        "target_column": cluster[j][1],
                        "relation": "MOSTLYIS"
                    })
    with open('new_uploads.json', 'w', encoding='utf-8') as f:
        json.dump(uploads, f, indent=2, ensure_ascii=False)

    return uploads

def upload_relations_to_neo4j(uploads, uri, username=None, password=None):
    from neo4j import GraphDatabase
    if username:
        driver = GraphDatabase.driver(uri, auth=(username, str(password)))
    else:
        driver = GraphDatabase.driver(uri)
    session = driver.session()

    cmds = []
    for upload in uploads:
        cmd = f"MATCH (a:Column), (b:Column) WHERE a.table = '{upload['source_table']}' AND a.name = '{upload['source_column']}' AND b.table = '{upload['target_table']}' AND b.name = '{upload['target_column']}' MERGE (a)-[:{upload['relation']}]->(b) RETURN a,b "
        cmds.append(cmd)
        session.run(cmd)
    
    with open("/data/liyiru/mysql-graph/upload_cmd.json", 'w', encoding='utf-8') as f:
        json.dump(cmds, f, indent=2, ensure_ascii=False)

    print("GENERATE cmds")


'''def upload_relations_to_neo4j(uploads, uri, username=None, password=None):
    from neo4j import GraphDatabase
    if username:
        driver = GraphDatabase.driver(uri, auth=(username, str(password)))
    else:
        driver = GraphDatabase.driver(uri)
    session = driver.session()

    for upload in uploads:
        for i in range(len(upload)):
            for j in range(i + 1, len(upload)):
                data = session.run(f"MATCH (a)-[:IS]-(b) WHERE a.name = '{upload[i][1]}' AND a.table = '{upload[i][0]}' AND b.name = '{upload[j][1]}' AND b.table = '{upload[j][0]}' RETURN a, b")
                datad = data.data()

                if len(datad) == 0:
                    session.run(f"MATCH (a:Column), (b:Column) WHERE a.name = '{upload[i][1]}' AND a.table = '{upload[i][0]}' AND b.name = '{upload[j][1]}' AND b.table = '{upload[j][0]}' MERGE (a)-[:MOSTLYIS]->(b) RETURN a, b")

    for item in json_data:
        print(f"{item['fk_table']}.{item['fk_column']} TO {item['pk_table']}.{item['pk_column']}")
        session.run(f"MATCH (a:Column), (b:Column) WHERE b.name = '{item['fk_column']}' AND b.table = '{item['fk_table']}' AND a.name = '{item['pk_column']}' AND a.table = '{item['pk_table']}' MERGE (a)-[:IS]->(b) RETURN a,b")'''

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


# 使用示例
if __name__ == "__main__":
    parser = ArgumentParser(description="Transfer")
    parser.add_argument("-c", "--config", type=str, required=True, help="Transfer Config Path")

    args = parser.parse_args()
    configs = TransferConfig(args.config)
    generate(configs)
    '''with open('/data/liyiru/mysql-graph/pre_cluster.json') as f:
        file = json.load(f)
    form_relationships_from_clusters(file)
'''


    # dot_file = "er_full_diagram.dot"
    # json_file = "er_full_diagram_pygraphviz.json"
    # generate_er('172.31.24.111', 3307, 'root', 'liucd123', 'netcare', '172.31.24.111', 7688, 'neo4j', '12345678', json_file_path=json_file)
    # generate_er('172.31.26.206', 3306, 'ai_test', 'Netcare@13579', 'netcaredb_ai', '172.31.24.111', 7688, 'neo4j', '12345678', json_file_path=json_file)
    # json_data = dot_to_json_pygraphviz(dot_file, json_file)
    # print(json_data)
    # dict1 = {"label": "<FONT FACE=\"Helvetica\"><TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLPADDING=\"4\" CELLSPACING=\"0\"><TR><TD><B><FONT POINT-SIZE=\"16\">framework_user_role</FONT></B></TD></TR><TR><TD ALIGN=\"LEFT\" PORT=\"ROLE_ID\"><u><FONT>ROLE_ID</FONT></u> <FONT> [INTEGER]</FONT> NOT NULL</TD></TR><TR><TD ALIGN=\"LEFT\" PORT=\"USER_ID\"><u><FONT>USER_ID</FONT></u> <FONT> [INTEGER]</FONT> NOT NULL</TD></TR></TABLE></FONT>"}
    # parse_attr(dict1)
    # upload_to_neo4j(json_data, 'bolt://172.31.24.111:7688', 'neo4j', '12345678', 'init')
