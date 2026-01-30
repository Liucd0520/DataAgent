"""
FastAPI 主应用
"""


import sys
from pathlib import Path
# 将项目根目录添加到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.app_schema import router as schema_router
from api.app_web_search import router as web_search_router
from api.app_milvus import router as milvus_router


# 创建 FastAPI 应用实例
app = FastAPI(
    title="DataAgent API",
    description="数据处理智能代理 API",
    version="1.0.0"
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源，生产环境应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 注册路由
app.include_router(schema_router)
app.include_router(web_search_router)
app.include_router(milvus_router)


@app.get("/")
async def root():
    """根路径"""
    return {
        "message": "Welcome to DataAgent API",
        "docs": "/docs",
        "version": "1.0.0"
    }


@app.get("/health")
async def health_check():
    """健康检查"""
    return {
        "status": "healthy",
        "service": "DataAgent API"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=False
    )
