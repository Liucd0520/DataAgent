"""
联网搜索相关的 API 路由
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any
from pydantic import BaseModel, Field
from services.web_search_service import search_term_explanation


# 定义请求模型
class WebSearchRequest(BaseModel):
    """联网搜索请求"""
    name: str = Field(..., description="要搜索的术语名称")


# 定义响应模型
class WebSearchResponse(BaseModel):
    """联网搜索响应"""
    success: bool = Field(..., description="是否成功")
    explanation: str = Field(..., description="术语解释")


# 创建路由器
router = APIRouter(
    prefix="/api/web-search",
    tags=["web-search"]
)


@router.post("/search", response_model=WebSearchResponse)
async def web_search_endpoint(request: WebSearchRequest) -> Dict[str, Any]:
    """
    联网搜索术语解释

    功能：
    通过联网搜索获取术语的详细解释和说明

    Args:
        request: 包含要搜索术语的请求

    Returns:
        WebSearchResponse: 包含成功标志和术语解释的响应

    示例:
        请求: {"name": "ARPU"}
        响应: {
            "success": true,
            "explanation": "ARPU (Average Revenue Per User) 即每用户平均收入..."
        }
    """
    try:
        # 调用服务层处理
        result = search_term_explanation(term=request.name)

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"搜索失败: {str(e)}")


@router.get("/health")
async def health_check():
    """健康检查接口"""
    return {"status": "ok", "message": "Web Search API is running"}
