from pymilvus import MilvusClient, DataType
from langchain_openai import OpenAIEmbeddings
from typing import List, Dict, Any
import numpy as np
from openai import OpenAI
from pymilvus import AnnSearchRequest
from pymilvus import RRFRanker, WeightedRanker
from pymilvus.model.hybrid import BGEM3EmbeddingFunction

class MilvusOperation(object):
    """
    Milvus 操作类

    功能包括：
    1. 创建/删除集合
    2. 批量插入数据
    3. 批量删除数据
    4. 向量检索（稠密、稀疏、混合）
    5. 过滤查询
    """

    def __init__(self, uri: str, collection_name: str, model_path: str, device: str):
        """
        初始化 Milvus 客户端

        Args:
            uri: Milvus 服务器地址
            collection_name: 集合名称
            model_path: 嵌入模型路径
            device: 设备类型 ('cuda:0', 'cpu' 等)
        """
        self.client = MilvusClient(uri=uri)
        self.collection_name = collection_name
        self.ef = BGEM3EmbeddingFunction(model_name=model_path, use_fp16=False, device=device)

        self.ranker = RRFRanker(100) # WeightedRanker(1.0, 1.0)

    def get_embeddings(self, texts: List[str]):
        """
        获取文本的稠密和稀疏嵌入向量

        Args:
            texts: 文本列表

        Returns:
            dict: 包含 'dense' 和 'sparse' 的字典
        """
        return self.ef(texts)

    def upsert_batch(self, data_list: List[Dict[str, Any]],  batch_size: int = 1000) -> Dict[str, Any]:
        """
        批量插入/更新数据到 Milvus

        Args:
            data_list: 数据列表
            batch_size: 批次大小，默认 1000

        Returns:
            dict: 插入结果统计信息
        """
        if not data_list:
            return {"success": 0, "failed": 0, "total": 0, "message": "没有数据需要插入"}

        total_count = len(data_list)
        success_count = 0
        failed_count = 0

        print(f"开始插入 {total_count} 条数据到集合 {self.collection_name}")

        # 分批处理
        for idx in range(0, total_count, batch_size):
            import time
            start = time.time()
            batch_data = data_list[idx: idx + batch_size]

            try:
                # 1. 提取文本
                batch_docs = [each_data.get('query', "") for each_data in batch_data]

                # 2. 生成嵌入向量
                docs_embeddings = self.get_embeddings(batch_docs)

                # 3. 准备插入数据（添加向量）
                batch_insert_data = []
                for i, each_data in enumerate(batch_data):
                    dense_emb = docs_embeddings['dense'][i]
                    sparse_emb = docs_embeddings['sparse']._getrow(i)

                    # 创建新字典，添加向量字段
                    data_with_vector = each_data.copy()
                    data_with_vector.update({
                        "sparse": sparse_emb,
                        "dense": dense_emb
                    })
                    batch_insert_data.append(data_with_vector)

                # 4. 执行插入
                self.client.upsert(
                    collection_name=self.collection_name,
                    data=batch_insert_data
                )

                success_count += len(batch_data)
                print(f"批次 {idx // batch_size + 1}: 成功插入 {len(batch_data)} 条数据，耗时 {time.time() - start:.2f} 秒")

            except Exception as e:
                print(f"批次 {idx // batch_size + 1} 插入失败: {str(e)}")
                failed_count += len(batch_data)

        result = {
            "total": total_count,
            "success": success_count,
            "failed": failed_count,
            "message": f"完成插入 {total_count} 条数据，成功 {success_count} 条，失败 {failed_count} 条"
        }

        print(result["message"])
        return result

    def delete_batch(self, ids: List[str]) -> Dict[str, Any]:
        """
        批量删除数据

        Args:
            ids: 要删除的主键 ID 列表

        Returns:
            dict: 删除结果
        """
        if not ids:
            return {"success": 0, "failed": 0, "total": 0, "message": "没有提供要删除的 ID"}

        print(f"开始删除 {len(ids)} 条数据")

        try:
            # Milvus 使用 delete 方法删除数据
            # expr 格式: "id in ['id1', 'id2', 'id3']"
            ids_str = ", ".join([f"'{id}'" for id in ids])
            expr = f"id in [{ids_str}]"

            # 执行删除
            self.client.delete(
                collection_name=self.collection_name,
                expr=expr
            )

            result = {
                "success": len(ids),
                "failed": 0,
                "total": len(ids),
                "message": f"成功删除 {len(ids)} 条数据"
            }

            print(result["message"])
            return result

        except Exception as e:
            result = {
                "success": 0,
                "failed": len(ids),
                "total": len(ids),
                "message": f"删除失败: {str(e)}"
            }

            print(result["message"])
            return result

    

    def create_collection_if_exists_or_not(self, is_first=False):
        """
        创建Milvus集合
        - is_first=True: 第一次初始化项目，直接创建collection
        - is_first=False: 如果存在则删除后重建
        """
        # 检查集合是否存在
        collection_exists = False
        try:
            print('-=-=-=-=-=')
            self.client.describe_collection(collection_name=self.collection_name)
            collection_exists = True
            print(f"集合 {self.collection_name} 已存在")
        except Exception:
            print(f"集合 {self.collection_name} 不存在")

        # 根据is_first参数决定处理方式
        if is_first:
            # 第一次初始化，如果已存在则直接返回
            if collection_exists:
                print(f"第一次初始化，集合 {self.collection_name} 已存在，直接使用")
                return True
        else:
            # 非第一次初始化，如果存在则删除
            if collection_exists:
                print(f"删除现有集合 {self.collection_name}")
                self.client.drop_collection(collection_name=self.collection_name)


        # 获取向量维度
        dense_dim = self.ef.dim["dense"]

        # 创建schema
        schema = self.client.create_schema(
            auto_id=False,
            enable_dynamic_field=True
        ) 

        # 添加字段到schema
        schema.add_field(field_name="id", datatype=DataType.VARCHAR, max_length=100, is_primary=True)
        schema.add_field(field_name="query", datatype=DataType.VARCHAR, max_length=100)
        schema.add_field(field_name="answer", datatype=DataType.VARCHAR, max_length=1000)
        schema.add_field(field_name="createdAt", datatype=DataType.VARCHAR, max_length=100)
        schema.add_field(field_name="updatedAt", datatype=DataType.VARCHAR, max_length=100)
        schema.add_field(field_name="sparse", datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field(field_name="dense", datatype=DataType.FLOAT_VECTOR, dim=dense_dim)

        # 准备索引参数
        index_params = self.client.prepare_index_params()

        # 添加稠密向量索引
        index_params.add_index(
            field_name="dense",
            index_name="dense_index",
            index_type="IVF_FLAT",
            metric_type="IP",
            params={"nlist": 128}
        )

        # 添加稀疏向量索引
        index_params.add_index(
            field_name="sparse",
            index_name="sparse_index",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="IP",
            params={"drop_ratio_build": 0.2}
        )

        # 创建集合
        self.client.create_collection(
            collection_name=self.collection_name,
            schema=schema,
            index_params=index_params
        )
        print(f"集合 {self.collection_name} 创建成功")
        return True

    def collection_exists(self):
        """
        检查集合是否存在
        """
        try:
            self.client.describe_collection(collection_name=self.collection_name)
            return True
        except Exception:
            return False
        


    # 根据过滤条件查询
    def query_with_filter(self,  filter_exp='', output_fields=[], limit=5000):
        ret = self.client.query(collection_name=self.collection_name, filter=filter_exp, output_fields=output_fields, limit=limit)
        
        return [each_data for each_data in ret]   # query 似乎不支持多个查询,
        
    # 向量稠密检索
    def search_vector_filter(self, query,  filter_exp='', output_fields=[], limit=5000):
        vector = self.embedding_bge([query])
        res = self.client.search(collection_name=self.collection_name, data=vector, filter=filter_exp, limit=limit, output_fields=output_fields, )
        return res
    
    # 混合检索
    def search_hybrid(self, query: str, filter_exp: str = '', output_fields: list = [], limit: int = 5000):
        # 事项作为查询向量
        query_embedding = self.ef([query])
        query_dense_embedding = query_embedding['dense'][0]
        query_sparse_embedding = query_embedding['sparse'][[0]]

        search_param_1 = {
            "data": [query_dense_embedding],
            "anns_field": "dense",
            "param": {
                "metric_type": "IP",
                "params": {"nprobe": 10}
            },
            "limit": limit, 
            "expr": filter_exp
        }
        request_dense = AnnSearchRequest(**search_param_1)

        search_param_2 = {
            "data": [query_sparse_embedding],
            "anns_field": "sparse",
            "param": {
                "metric_type": "IP",
                "params": {"drop_ratio_build": 0.2}
            },
            "limit": limit,
            "expr": filter_exp 
        }
        request_sparse = AnnSearchRequest(**search_param_2)
        
        reqs = [request_dense, request_sparse]

        search_result = self.client.hybrid_search(
            collection_name=self.collection_name,
            reqs=reqs,
            ranker=self.ranker,
            limit=limit,
            output_fields=output_fields
        )
        
        return search_result 





if __name__ == '__main__':


    collection_name = 'hello_tmp'
    uri = 'http://172.31.24.111:19534'

    opt = MilvusOperation(uri=uri, collection_name=collection_name,device="cuda:7", 
                          model_path='/data/models/embeddings/bge-m3')
    
    question = '司机绕路'
    res = opt.search_hybrid( 
                            query=question, 
                            filter_exp='summary like "%网约车司机%"  OR summary like "%网约车驾驶员%"  OR summary like "%滴滴司机%"  OR summary like "%出租车驾驶员%"  OR summary like "%网约车驾驶员%" ', 
                            output_fields=['summary', ],
                            limit=10000)
    res = res[0]
    print(len(res) ,'\n'.join([each['entity']['summary'] for each in res[:10]]))