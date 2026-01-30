"""
Milvus 向量库操作相关的 API 路由
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from services.milvus_service import milvus_batch_operation


# 定义请求模型
class MilvusUpsertRequest(BaseModel):
    """Milvus 批量插入/更新请求"""
    collection_name: str = Field(..., description="集合名称")
    operation: str = Field(default="upsert", description="操作类型，固定为 'upsert'")
    data: List[Dict[str, Any]] = Field(..., description="要插入/更新的数据列表")


class MilvusDeleteRequest(BaseModel):
    """Milvus 批量删除请求"""
    collection_name: str = Field(..., description="集合名称")
    operation: str = Field(default="delete", description="操作类型，固定为 'delete'")
    ids: List[str] = Field(..., description="要删除的 ID 列表")


# 定义响应模型
class MilvusOperationResponse(BaseModel):
    """Milvus 操作响应"""
    success: bool = Field(..., description="操作是否成功")
    total: int = Field(default=0, description="总数据量")
    success_count: int = Field(default=0, description="成功数量")
    failed_count: int = Field(default=0, description="失败数量")
    message: str = Field(..., description="操作结果消息")


# 创建路由器
router = APIRouter(
    prefix="/api/milvus",
    tags=["milvus"]
)


@router.post("/upsert", response_model=MilvusOperationResponse)
async def milvus_upsert_endpoint(request: MilvusUpsertRequest) -> Dict[str, Any]:
    """
    Milvus 批量插入/更新数据

    功能：
    批量将数据插入或更新到指定的 Milvus 集合中

    Args:
        request: 包含 collection_name 和 data 的请求

    Returns:
        MilvusOperationResponse: 操作结果

    示例:
        请求: {
            "collection_name": "hello_milvus",
            "operation": "upsert",
            "data": [
                {"id": "1", "query": "测试问题1", "answer": "测试答案1"},
                {"id": "2", "query": "测试问题2", "answer": "测试答案2"}
            ]
        }
        响应: {
            "success": true,
            "total": 2,
            "success_count": 2,
            "failed_count": 0,
            "message": "完成插入 2 条数据，成功 2 条，失败 0 条"
        }
    """
    try:
        # 调用服务层处理
        result = milvus_batch_operation(
            collection_name=request.collection_name,
            operation="upsert",
            data=request.data
        )

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"操作失败: {str(e)}")


@router.post("/delete", response_model=MilvusOperationResponse)
async def milvus_delete_endpoint(request: MilvusDeleteRequest) -> Dict[str, Any]:
    """
    Milvus 批量删除数据

    功能：
    根据 ID 列表批量删除指定集合中的数据

    Args:
        request: 包含 collection_name 和 ids 的请求

    Returns:
        MilvusOperationResponse: 操作结果

    示例:
        请求: {
            "collection_name": "hello_milvus",
            "operation": "delete",
            "ids": ["1", "2", "3"]
        }
        响应: {
            "success": true,
            "total": 3,
            "success_count": 3,
            "failed_count": 0,
            "message": "成功删除 3 条数据"
        }
    """
    try:
        # 调用服务层处理
        result = milvus_batch_operation(
            collection_name=request.collection_name,
            operation="delete",
            ids=request.ids
        )

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"操作失败: {str(e)}")


@router.get("/health")
async def health_check():
    """健康检查接口"""
    return {"status": "ok", "message": "Milvus API is running"}
