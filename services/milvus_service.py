"""
Milvus 向量库操作服务层
"""

from typing import Dict, List, Any
from DataAgent.knowledge.milvus_client import MilvusOperation
from config import config


def milvus_batch_operation(
    collection_name: str,
    operation: str,
    data: List[Dict[str, Any]] = None,
    ids: List[str] = None
) -> Dict[str, Any]:
    """
    Milvus 批量操作（upsert 或 delete）

    Args:
        collection_name: 集合名称
        operation: 操作类型，'upsert' 或 'delete'
        data: upsert 操作的数据列表
        ids: delete 操作的 ID 列表

    Returns:
        Dict[str, Any]: 操作结果
    """
    try:
        # 从配置中获取 Milvus 连接参数
        uri = config.MILVUS_URI
        model_path = config.MILVUS_MODEL_PATH
        device = config.MILVUS_DEVICE

        # 初始化 Milvus 操作客户端
        milvus_op = MilvusOperation(
            uri=uri,
            collection_name=collection_name,
            model_path=model_path,
            device=device
        )

        # 检查集合是否存在
        if not milvus_op.collection_exists():
            return {
                "success": False,
                "message": f"集合 '{collection_name}' 不存在"
            }

        # 根据操作类型执行相应的操作
        if operation == "upsert":
            if data is None or not data:
                return {
                    "success": False,
                    "message": "upsert 操作需要提供 data 参数"
                }

            # 执行批量插入/更新
            result = milvus_op.upsert_batch(data_list=data)
            return {
                "success": result["success"] > 0,
                "total": result["total"],
                "success_count": result["success"],
                "failed_count": result["failed"],
                "message": result["message"]
            }

        elif operation == "delete":
            if ids is None or not ids:
                return {
                    "success": False,
                    "message": "delete 操作需要提供 ids 参数"
                }

            # 执行批量删除
            result = milvus_op.delete_batch(ids=ids)
            return {
                "success": result["success"] > 0,
                "total": result["total"],
                "success_count": result["success"],
                "failed_count": result["failed"],
                "message": result["message"]
            }

        else:
            return {
                "success": False,
                "message": f"不支持的操作类型: {operation}，仅支持 'upsert' 或 'delete'"
            }

    except Exception as e:
        return {
            "success": False,
            "message": f"操作失败: {str(e)}"
        }
