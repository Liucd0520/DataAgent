"""
数据库到 Milvus 数据同步类

支持的数据库: MySQL, PostgreSQL, 达梦(DM), TeleDB

功能:
1. 自动获取数据库表的字段信息（支持多种方言）
2. 根据数据库字段创建Milvus collection schema
3. 分批次从数据库读取数据
4. 支持初始导入和增量更新(upsert)
5. 自动处理向量字段的嵌入生成
"""

from langchain_community.utilities import SQLDatabase
from pymilvus import MilvusClient, FieldSchema, CollectionSchema, DataType
from typing import List, Dict, Any, Optional, Tuple
import logging
from tqdm import tqdm
import datetime 
from decimal import Decimal
import decimal


logger = logging.getLogger(__name__)


class MySQLToMilvusDumper:
    """
    数据库到Milvus的数据同步器

    支持的数据库类型:
    - MySQL
    - PostgreSQL
    - DM (达梦数据库)
    - TeleDB (PostgreSQL兼容)

    使用示例:
        dumper = MySQLToMilvusDumper(
            mysql_config={...},
            milvus_config={...},
            table_name="users",
            collection_name="users_vectors",
            vector_field="description_embedding",
            batch_size=1000
        )

        # 初始导入
        dumper.initial_import()

        # 增量更新
        dumper.incremental_sync(last_sync_time="2024-01-01 00:00:00")
    """

    def __init__(
        self,
        mysql_db: SQLDatabase,
        milvus_config: Dict[str, Any],
        table_name: str,
        collection_name: str,
        id_field: str = "id",
        text_field: Optional[str] = None,
        embedding_model: Optional[Any] = None,
        batch_size: int = 1000,
        primary_key_field: str = "id",
        db_type: str = "mysql",
        dense_dim: int = 1024,
        enable_sparse: bool = False,
        field_name_mapping: Optional[Dict[str, str]] = None
    ):
        """
        初始化配置

        Args:
            mysql_db: SQLDatabase实例 (LangChain的SQLDatabase对象)
                使用方式: db = SQLDatabase.from_uri(uri)
                支持的数据库类型:
                - MySQL: "mysql+mysqlconnector://root:password@host:port/database"
                - PostgreSQL: "postgresql+psycopg2://user:password@host:port/database"
                - 达梦: "dm+dmPython://user:password@host:port/database"
                - TeleDB: "postgresql+psycopg2://user:password@host:port/database" (使用PostgreSQL驱动)
            milvus_config: Milvus连接配置
                {
                    'uri': 'http://localhost:19530'
                }
            table_name: 数据库源表名
            collection_name: Milvus目标collection名
            id_field: 数据库的主键字段名（数据库中的字段名，可能是中文）
            text_field: 用于生成向量的文本字段名（数据库中的字段名，可能是中文）
            embedding_model: 向量嵌入模型实例
            batch_size: 批次大小
            primary_key_field: Milvus collection的主键字段名（映射后的英文字段名，默认与id_field相同）
            db_type: 数据库类型，支持 'mysql', 'postgresql', 'dm', 'teledb'
            dense_dim: 稠密向量维度，默认1024
            enable_sparse: 是否启用稀疏向量，默认False
            field_name_mapping: 字段名映射表 {中文字段名: 英文字段名}
                              例如: {"工单编号": "work_order_id", "内容描述": "description"}
                              如果为None，则不进行映射
        """
        self.mysql_db = mysql_db
        self.milvus_config = milvus_config
        self.table_name = table_name
        self.collection_name = collection_name
        self.dense_vector_field = "dense"  # 稠密向量字段名
        self.sparse_vector_field = "sparse"  # 稀疏向量字段名
        self.id_field = id_field
        self.text_field = text_field
        self.embedding_model = embedding_model
        self.batch_size = batch_size
        self.primary_key_field = primary_key_field
        self.db_type = db_type.lower()
        self.dense_dim = dense_dim
        self.enable_sparse = enable_sparse
        self.field_name_mapping = field_name_mapping or {}  # 字段名映射表

        # 连接对象
        self.milvus_client = None

    def _map_field_name(self, original_field_name: str) -> str:
        """
        将原始字段名（可能是中文）映射为英文字段名

        Args:
            original_field_name: 原始字段名（数据库中的字段名）

        Returns:
            映射后的字段名，如果映射表中不存在则返回原始字段名
        """
        return self.field_name_mapping.get(original_field_name, original_field_name)

    def _get_quote_char(self) -> str:
        """
        根据数据库方言获取合适的引号符

        支持的数据库: mysql, postgresql, dm (达梦), teledb (PostgreSQL兼容)

        Returns:
            引号符: MySQL用反引号, PostgreSQL/达梦/teledb用双引号, 其他默认反引号
        """
        dialect = self.mysql_db.dialect.lower()

        # PostgreSQL, 达梦, teledb 都使用双引号
        if dialect in ['postgresql', 'postgres', 'dm', 'dameng', 'teledb']:
            return '"'  # PostgreSQL、达梦和 teledb 使用双引号
        elif dialect == 'mysql':
            return '`'  # MySQL 使用反引号
        else:
            return '`'  # 默认使用 MySQL 风格

    def connect_milvus(self) -> MilvusClient:
        """建立Milvus连接"""
        if self.milvus_client is None:
            self.milvus_client = MilvusClient(
                uri=self.milvus_config.get('uri', 'http://172.31.24.111:19534')
            )
        return self.milvus_client

    def get_mysql_table_schema(self) -> Tuple[List[str], List[str]]:
        """
        获取数据库表的字段信息（支持多种数据库方言）

        支持的数据库: mysql, postgresql, dm (达梦), teledb (PostgreSQL兼容)

        Returns:
            (field_list, type_list) - 字段名列表和类型列表
        """
        quote = self._get_quote_char()
        dialect = self.mysql_db.dialect.lower()

        # 根据方言选择不同的查询方式
        if dialect == 'mysql':
            # MySQL 使用 SHOW FIELDS
            cmd = f'SHOW FIELDS FROM {quote}{self.table_name}{quote};'
        elif dialect in ['postgresql', 'postgres', 'teledb']:
            # PostgreSQL 和 teledb 使用 information_schema
            cmd = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{self.table_name}'
            AND table_schema = 'public';
            """
        elif dialect in ['dm', 'dameng']:
            # 达梦数据库使用 information_schema（类似于 PostgreSQL）
            cmd = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{self.table_name}';
            """
        else:
            # 默认使用 information_schema（通用方式）
            cmd = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{self.table_name}';
            """

        logger.info(f"执行SQL (方言={dialect}): {cmd.strip()}")

        result = eval(self.mysql_db.run(cmd))

        field_list, type_list = [], []
        for item in result:
            mysql_field, mysql_type = item[0], item[1]
            field_list.append(mysql_field)
            type_list.append(mysql_type)

        logger.info(f"获取到表 {self.table_name} 的 {len(field_list)} 个字段")
        logger.info(f"字段: {field_list}")
        logger.info(f"类型: {type_list}")

        return field_list, type_list

    def mysql_type_to_milvus_type(self, mysql_type: str) -> DataType:
        """
        将MySQL数据类型映射到Milvus数据类型

        Args:
            mysql_type: MySQL数据类型 (如 'varchar(255)', 'int', 'text')

        Returns:
            Milvus DataType
        """
        type_mapping = {
            'text': DataType.VARCHAR,
            'longtext': DataType.VARCHAR,
            'bigint': DataType.INT64,
            'tinyint': DataType.INT32,
            'smallint': DataType.INT32,
            'mediumint': DataType.INT32,
            'float': DataType.FLOAT,
            'double': DataType.DOUBLE,
            'decimal': DataType.DOUBLE,
            'boolean': DataType.BOOL,
            'enum': DataType.VARCHAR,
            'set': DataType.VARCHAR,
            'datetime': DataType.VARCHAR,
            'char': DataType.VARCHAR,
            'varchar': DataType.VARCHAR,
            'json': DataType.JSON,
            'int': DataType.INT64,
            'timestamp': DataType.VARCHAR,
            'date': DataType.VARCHAR,
            'boolean': DataType.BOOL,
        }

        # 处理类型名称,去除括号中的长度信息
        base_type = mysql_type.lower().split('(')[0]

        # 特殊处理 tinyint(1) 这种情况
        if 'tinyint(1)' in mysql_type.lower():
            return DataType.INT32

        return type_mapping.get(base_type, DataType.VARCHAR)

    def create_milvus_collection_schema(self, field_list: List[str], type_list: List[str]) -> CollectionSchema:
        """
        根据数据库表结构创建Milvus Collection Schema

        Args:
            field_list: 数据库字段名列表（可能是中文）
            type_list: 数据库字段类型列表

        Returns:
            Milvus CollectionSchema对象
        """
        fields = []

        for field_name, field_type in zip(field_list, type_list):
            print(field_name)
            # 跳过向量字段(会在后面添加)
            if field_name in [self.dense_vector_field, self.sparse_vector_field]:
                continue

            milvus_type = self.mysql_type_to_milvus_type(field_type)
            print(field_type, milvus_type)

            # 将字段名映射为英文
            mapped_field_name = self._map_field_name(field_name)

            # 判断是否是主键
            is_primary = (field_name == self.id_field)

            # 只为VARCHAR类型设置max_length参数
            extra_params = {}
            if milvus_type == DataType.VARCHAR:
                extra_params['max_length'] = 65535

            field_schema = FieldSchema(
                name=mapped_field_name,  # 使用映射后的字段名
                dtype=milvus_type,
                is_primary=is_primary,
                auto_id=False,
                nullable=False if is_primary else True,  # 非主键允许为NULL
                **extra_params
            )
            fields.append(field_schema)

        # 添加稠密向量字段
        dense_vector_field_schema = FieldSchema(
            name=self.dense_vector_field,
            dtype=DataType.FLOAT_VECTOR,
            dim=self.dense_dim
        )
        fields.append(dense_vector_field_schema)

        # 如果启用稀疏向量，添加稀疏向量字段
        if self.enable_sparse:
            sparse_vector_field_schema = FieldSchema(
                name=self.sparse_vector_field,
                dtype=DataType.SPARSE_FLOAT_VECTOR
            )
            fields.append(sparse_vector_field_schema)

        schema = CollectionSchema(
            fields=fields,
            description=f"Collection migrated from {self.db_type} table: {self.table_name}",
            enable_dynamic_field=True
        )

        sparse_info = ", 包含稀疏向量" if self.enable_sparse else ""
        logger.info(f"创建Milvus Collection Schema, 包含 {len(fields)} 个字段, 稠密向量维度: {self.dense_dim}{sparse_info}")
        return schema

    def build_milvus_index_params(self) -> Any:
        """
        构建Milvus索引参数

        Returns:
            index_params对象
        """
        client = self.connect_milvus()

        # Prepare index parameters
        index_params = client.prepare_index_params()

        # 为稠密向量字段添加索引
        index_params.add_index(
            field_name=self.dense_vector_field,
            index_name=f"{self.dense_vector_field}_index",
            index_type="IVF_FLAT",
            metric_type="IP",
            params={"nlist": 128}
        )

        # 如果启用稀疏向量，为稀疏向量字段添加索引
        if self.enable_sparse:
            index_params.add_index(
                field_name=self.sparse_vector_field,
                index_name=f"{self.sparse_vector_field}_index",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="IP",
                params={"drop_ratio_build": 0.2}
            )

        return index_params

    def create_milvus_collection(self) -> None:
        """创建Milvus Collection"""
        client = self.connect_milvus()

        # 检查collection是否已存在
        if client.has_collection(self.collection_name):
            logger.info(f"Collection {self.collection_name} 已存在,将使用upsert模式")
            return

        # 获取MySQL表结构
        field_list, type_list = self.get_mysql_table_schema()

        # 创建schema
        schema = self.create_milvus_collection_schema(field_list, type_list)

        # 构建索引参数
        index_params = self.build_milvus_index_params()

        # 创建collection
        client.create_collection(
            collection_name=self.collection_name,
            schema=schema,
            index_params=index_params
        )

        logger.info(f"成功创建Collection: {self.collection_name}")

    def generate_embeddings(self, texts: List[str]) -> Dict[str, Any]:
        """
        批量生成文本嵌入（稠密向量和稀疏向量）

        Args:
            texts: 输入文本列表

        Returns:
            包含 'dense' 和 'sparse' 的字典
            例如: {'dense': [...], 'sparse': {...}}
        """
        if self.embedding_model is None:
            raise ValueError("未设置dense_embedding_model,无法生成向量")

        # 调用嵌入模型函数（如BGEM3EmbeddingFunction）
        # 该函数应返回: {'dense': [...], 'sparse': ...}
        embeddings = self.embedding_model(texts)

        return embeddings

    def fetch_mysql_data_batch(
        self,
        offset: int = 0,
        limit: Optional[int] = None,
        where_condition: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        分批次从数据库获取数据（支持多种方言）

        支持的数据库: mysql, postgresql, dm (达梦), teledb (PostgreSQL兼容)

        Args:
            offset: 偏移量
            limit: 限制条数
            where_condition: 额外的WHERE条件

        Returns:
            数据列表
        """
        quote = self._get_quote_char()
        dialect = self.mysql_db.dialect.lower()

        # 构建基础SQL（使用正确的引号符）
        sql = f"SELECT * FROM {quote}{self.table_name}{quote}"

        if where_condition:
            sql += f" WHERE {where_condition}"

        # 根据方言添加分页语法
        if dialect in ['mysql', 'postgresql', 'postgres', 'dm', 'dameng', 'teledb']:
            # MySQL, PostgreSQL, 达梦, teledb: 使用 LIMIT/OFFSET
            if limit is not None:
                sql += f" LIMIT {limit} OFFSET {offset}"
        elif dialect in ['mssql', 'sqlserver', 'microsoft']:
            # SQL Server: 使用 OFFSET/FETCH
            if limit is not None:
                sql += f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        else:
            # 默认使用 LIMIT/OFFSET
            if limit is not None:
                sql += f" LIMIT {limit} OFFSET {offset}"

        logger.debug(f"执行SQL (方言={dialect}): {sql}")

        # 执行SQL并解析结果
        result_str = self.mysql_db.run(sql)
        data = eval(result_str)

        # 将元组列表转换为字典列表
        field_list, _ = self.get_mysql_table_schema()
        dict_data = []
        for row in data:
            row_dict = dict(zip(field_list, row))
            dict_data.append(row_dict)

        return dict_data

    def get_total_count(self, where_condition: Optional[str] = None) -> int:
        """
        获取数据总数（支持多方言）

        支持的数据库: mysql, postgresql, dm (达梦), teledb (PostgreSQL兼容)
        """
        quote = self._get_quote_char()

        # 构建SQL（使用正确的引号符）
        sql = f"SELECT COUNT(*) as total FROM {quote}{self.table_name}{quote}"
        if where_condition:
            sql += f" WHERE {where_condition}"

        logger.debug(f"执行SQL: {sql}")

        result_str = self.mysql_db.run(sql)
        result = eval(result_str)

        return result[0][0] if result else 0

    def prepare_data_for_milvus(self, raw_data: List[Dict]) -> List[Dict[str, Any]]:
        """
        准备插入Milvus的数据
        - 批量生成向量嵌入（稠密和稀疏）
        - 转换数据类型（datetime -> str, Decimal -> float）
        - 处理空值

        Args:
            raw_data: 数据库原始数据

        Returns:
            处理后的数据
        """
        prepared_data = []

        if not raw_data:
            return prepared_data

        # 批量生成嵌入向量
        if self.text_field:
            # 提取所有文本
            texts = []
            for row in raw_data:
                if self.text_field in row and row[self.text_field]:
                    texts.append(str(row[self.text_field]))
                else:
                    texts.append("")  # 空文本

            # 批量调用嵌入模型
            docs_embeddings = self.generate_embeddings(texts)

            # 准备数据
            for i, row in enumerate(raw_data):
                data_row = {}

                # 复制标量字段并转换类型
                for key, value in row.items():
                    if key not in [self.dense_vector_field, self.sparse_vector_field]:
                        # 转换数据类型以适配 Milvus
                        # 将中文字段名映射为英文字段名
                        mapped_key = self._map_field_name(key)
                        data_row[mapped_key] = self._convert_value_for_milvus(value)

                # 添加稠密向量
                dense_emb = docs_embeddings['dense'][i]
                data_row[self.dense_vector_field] = dense_emb

                # 如果启用稀疏向量，添加稀疏向量
                if self.enable_sparse:
                    sparse_emb = docs_embeddings['sparse']._getrow(i)
                    data_row[self.sparse_vector_field] = sparse_emb

                prepared_data.append(data_row)
        else:
            # 如果没有指定文本字段，直接复制数据（但需要类型转换）
            for row in raw_data:
                data_row = {}
                for key, value in row.items():
                    # 转换数据类型以适配 Milvus
                    # 将中文字段名映射为英文字段名
                    mapped_key = self._map_field_name(key)
                    data_row[mapped_key] = self._convert_value_for_milvus(value)
                prepared_data.append(data_row)

        return prepared_data

    def _convert_value_for_milvus(self, value: Any) -> Any:
        """
        转换值以适配 Milvus 数据类型

        Args:
            value: 原始值

        Returns:
            转换后的值
        """
        # 处理 None 值 - 转换为空字符串（Milvus 需要 VARCHAR 字段有值，即使是空字符串）
        if value is None:
            return None

        # 处理空字符串 - 保持为空字符串
        if isinstance(value, str) and value.strip() == '':
            return ""

        # 处理 datetime 对象（包括 datetime.datetime 和 datetime.date）
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value.isoformat()

        # 处理 Decimal 对象（转换为 float）
        if isinstance(value, Decimal):
            return float(value)

        # 处理其他特殊类型（如果需要）
        # 例如：bytes -> str (base64编码)

        return value

    def upsert_batch(self, data: List[Dict[str, Any]]) -> None:
        """
        批量插入/更新数据到Milvus (使用upsert，存在则更新，不存在则插入)

        Args:
            data: 数据列表
        """
        if not data:
            return

        client = self.connect_milvus()

        try:
            client.upsert(
                collection_name=self.collection_name,
                data=data
            )
            logger.debug(f"成功upsert {len(data)} 条记录")
        except Exception as e:
            logger.error(f"Upsert数据失败: {e}")
            raise

    def initial_import(self, progress_callback: Optional[Any] = None) -> Dict[str, Any]:
        """
        初始导入:从MySQL导入所有数据到Milvus

        Args:
            progress_callback: 进度回调函数

        Returns:
            导入统计信息
        """
        logger.info(f"开始初始导入: {self.table_name} -> {self.collection_name}")

        # 创建collection
        self.create_milvus_collection()

        # 获取总数据量
        total_count = self.get_total_count()
        logger.info(f"总数据量: {total_count} 条")

        # 分批读取和插入
        offset = 0
        success_count = 0
        failed_count = 0

        with tqdm(total=total_count, desc="导入进度") as pbar:
            while offset < total_count:
                # 获取一批数据
                batch_data = self.fetch_mysql_data_batch(
                    offset=offset,
                    limit=self.batch_size
                )

                if not batch_data:
                    break

                try:
                    # 准备数据
                    prepared_data = self.prepare_data_for_milvus(batch_data)

                    # 插入/更新数据 (使用upsert)
                    self.upsert_batch(prepared_data)
                    success_count += len(prepared_data)

                except Exception as e:
                    logger.error(f"批次插入失败 (offset={offset}): {e}")
                    failed_count += len(batch_data)

                # 更新进度
                offset += self.batch_size
                pbar.update(len(batch_data))

                if progress_callback:
                    progress_callback(offset, total_count)

        stats = {
            'total': total_count,
            'success': success_count,
            'failed': failed_count,
            'collection_name': self.collection_name
        }

        logger.info(f"初始导入完成: {stats}")
        return stats

    def incremental_sync(
        self,
        last_sync_time: Optional[str] = None,
        update_column: str = "updated_at"
    ) -> Dict[str, Any]:
        """
        增量同步:同步指定时间之后更新的数据

        Args:
            last_sync_time: 上次同步时间 (格式: 'YYYY-MM-DD HH:MM:SS')
            update_column: 更新时间字段名

        Returns:
            同步统计信息
        """
        logger.info(f"开始增量同步: {self.table_name} -> {self.collection_name}")

        # 构建WHERE条件
        if last_sync_time:
            where_condition = f"{update_column} > '{last_sync_time}'"
        else:
            where_condition = None

        # 获取增量数据量
        total_count = self.get_total_count(where_condition)

        if total_count == 0:
            logger.info("没有需要同步的数据")
            return {'total': 0, 'success': 0, 'failed': 0}

        logger.info(f"增量数据量: {total_count} 条")

        # 分批处理
        offset = 0
        success_count = 0
        failed_count = 0

        with tqdm(total=total_count, desc="同步进度") as pbar:
            while offset < total_count:
                batch_data = self.fetch_mysql_data_batch(
                    offset=offset,
                    limit=self.batch_size,
                    where_condition=where_condition
                )

                if not batch_data:
                    break

                try:
                    prepared_data = self.prepare_data_for_milvus(batch_data)
                    self.upsert_batch(prepared_data)
                    success_count += len(prepared_data)

                except Exception as e:
                    logger.error(f"批次upsert失败 (offset={offset}): {e}")
                    failed_count += len(batch_data)

                offset += self.batch_size
                pbar.update(len(batch_data))

        stats = {
            'total': total_count,
            'success': success_count,
            'failed': failed_count,
            'sync_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        logger.info(f"增量同步完成: {stats}")
        return stats


    def full_sync(self) -> Dict[str, Any]:
        """
        全量同步:先删除旧数据,然后重新导入

        注意:此操作会删除Milvus中的所有现有数据
        """
        logger.warning("执行全量同步,将删除现有数据")

        client = self.connect_milvus()

        # 删除collection
        if client.has_collection(self.collection_name):
            client.drop_collection(self.collection_name)
            logger.info(f"已删除collection: {self.collection_name}")

        # 重新导入
        return self.initial_import()

    def close(self) -> None:
        """关闭连接"""
        # MilvusClient 不需要显式关闭连接
        # 这里保留方法是为了兼容性
        self.milvus_client = None

    def __enter__(self):
        """支持with语句"""
        self.connect_milvus()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """支持with语句"""
        self.close()


# ============ 使用示例 ============

if __name__ == "__main__":
    from langchain_community.utilities import SQLDatabase
    from pymilvus.model.hybrid import BGEM3EmbeddingFunction

    # MySQL连接
    uri = "mysql+mysqlconnector://root:liucd123@127.0.0.1:3306/12345"
    db = SQLDatabase.from_uri(uri)

    MILVUS_CONFIG = {
        'uri': 'http://172.31.24.111:19534'
    }

    import torch 
    # 初始化嵌入模型（BGE-M3同时支持稠密和稀疏向量）
    ef = BGEM3EmbeddingFunction(
        model_name=r'C:\Users\19097\Desktop\BAAI\bge-m3',
        batch_size=16,
        use_fp16=False,
        device= 'cpu' #"cuda" if torch.cuda.is_available() else 'cpu'
    )
    dense_dim = ef.dim["dense"]

    # 字段名映射表
    FIELD_MAPPING = {
        "工单编号": "work_order_id",
        "工号": "employee_id",
        "工单生成时间": "work_order_created_at",
        "诉求地址": "complaint_address",
        "诉求区域": "complaint_area",
        "内容描述": "description",
        "工单类别": "work_order_category",
        "处理描述": "handling_description",
        "客户类型": "customer_type",
        "一级分类": "level_1_category",
        "二级分类": "level_2_category",
        "三级分类": "level_3_category",
        "四级分类": "level_4_category",
        "主办单位": "responsible_department",
        "是否匿名": "is_anonymous",
        "服务类型": "service_type",
        "通话编号": "call_id",
        "工单类型": "work_order_type",
        "新一级分类": "new_level_1_category",
        "新二级分类": "new_level_2_category",
        "新三级分类": "new_level_3_category",
        "新四级分类": "new_level_4_category",
        "新五级分类": "new_level_5_category"
    }


    # 创建dumper实例
    dumper = MySQLToMilvusDumper(
        mysql_db=db,
        milvus_config=MILVUS_CONFIG,
        table_name='shanghai',
        collection_name='shanghai_2_vex',
        text_field='内容描述',  # 用于生成向量的文本字段
        embedding_model=ef,  # 传入嵌入模型
        id_field='工单编号',
        batch_size=500,
        dense_dim=dense_dim,  # 稠密向量维度
        enable_sparse=True,  # 启用稀疏向量
        field_name_mapping=FIELD_MAPPING
    )

    # 使用with语句自动管理连接
    with dumper:
        # # 方式1: 初始导入(首次同步)
        # stats = dumper.initial_import()
        # print(f"导入完成: {stats}")

        # 方式2: 增量更新(后续同步)
        stats = dumper.incremental_sync(last_sync_time='2024-02-01 00:07:47', update_column='工单生成时间')
        print(f"同步完成: {stats}")

        # 方式3: 全量重新同步
        # stats = dumper.full_sync()
        # print(f"全量同步完成: {stats}")
