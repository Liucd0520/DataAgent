"""
Schema 处理相关的 API 路由
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any
from pydantic import BaseModel, Field
from services.schema_service import process_schema


# 定义请求模型
class EnumValueItem(BaseModel):
    """枚举值项"""
    name: str = Field(..., description="字段名称")
    values: list[str] = Field(..., description="枚举值列表")
    total_count: int = Field(..., description="总数")
    is_complete: bool = Field(..., description="是否完整")


class ColumnItem(BaseModel):
    """列定义项"""
    name: str = Field(..., description="字段名")
    type: str = Field(..., description="字段类型")
    comment: str = Field(default="", description="字段注释")
    englishName: str = Field(default="", description="英文字段名")


class TableInfo(BaseModel):
    """表信息"""
    name: str = Field(..., description="表名")
    comment: str = Field(default="", description="表注释")
    columns: list[ColumnItem] = Field(..., description="列定义列表")


class SchemaProcessRequest(BaseModel):
    """Schema 处理请求"""
    enumTopValues: list[EnumValueItem] = Field(..., description="枚举值列表")
    table: TableInfo = Field(..., description="表信息")


# 定义响应模型
class ColumnItemResponse(BaseModel):
    """列定义响应"""
    name: str
    type: str
    comment: str
    englishName: str


class TableInfoResponse(BaseModel):
    """表信息响应"""
    name: str
    comment: str
    columns: list[ColumnItemResponse]


class SchemaProcessResponse(BaseModel):
    """Schema 处理响应"""
    table: TableInfoResponse
    enumTopValues: list[EnumValueItem]


# 创建路由器
router = APIRouter(
    prefix="/api/schema",
    tags=["schema"]
)


@router.post("/process", response_model=SchemaProcessResponse)
async def process_schema_endpoint(request: SchemaProcessRequest) -> Dict[str, Any]:
    """
    处理 Schema 信息

    功能：
    1. 将枚举值写入到对应字段的 comment 中
    2. 调用大模型生成表描述
    3. 调用大模型翻译字段名

    Args:
        request: Schema 处理请求

    Returns:
        SchemaProcessResponse: 处理后的 Schema 数据
    """
    try:
        # 将请求转换为字典
        request_data = request.model_dump()

        # 调用服务层处理
        result = process_schema(request_data)
    
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"处理失败: {str(e)}")


@router.get("/health")
async def health_check():
    """健康检查接口"""
    return {"status": "ok", "message": "Schema API is running"}
