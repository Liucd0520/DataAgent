# DataAgent

DataAgent 是一个基于自然语言的数据查询与分析系统，通过工作流编排实现智能数据处理。系统支持自然语言查询解析、自动算子编排、多种数据处理算子（SQL查询、语义过滤、分类等）以及基于 LangGraph 的异步工作流执行。

## 项目结构

```
DataAgent/
├── api/                        # API 接口层
│   ├── __init__.py
│   └── admin.py                # 管理接口
│
├── config/                     # 配置文件
│   ├── config.py               # 应用配置（数据库、API、日志等）
│   └── logger.py               # 日志配置
│
├── DataAgent/                  # 核心业务模块
│   ├── datasource/             # 数据源管理
│   │   ├── __init__.py
│   │   ├── db_config_read.py   # 数据库配置读取
│   │   ├── schema_obtain.py    # Schema 获取
│   │   ├── schema_parse.py     # Schema 解析
│   │   ├── schema_enhance.py   # Schema 增强（枚举值、表描述）
│   │   ├── chain.py            # LangChain 链定义
│   │   ├── util.py             # 数据源工具函数
│   │   └── prompt/             # Prompt 模板目录
│   │       └── schema_prompt.py # Schema 相关提示词
│   │
│   ├── knowledge/              # 知识库管理
│   │   ├── __init__.py
│   │   └── util.py             # 知识库工具函数
│   │
│   ├── query/                  # 智能查询
│   │   ├── __init__.py
│   │   └── util.py             # 查询工具函数
│   │
│   ├── report/                 # 报告生成
│   │   ├── __init__.py
│   │   └── util.py             # 报告工具函数
│   │
│   └── workflow/               # 工作流管理 ⭐核心模块
│       ├── nl2flow/            # 自然语言到工作流转换
│       │   ├── __init__.py     # 模块初始化，自动加载算子
│       │   ├── nl_parser.py    # 自然语言解析器
│       │   ├── workflow_builder.py  # 工作流构建器
│       │   └── workflow_state.py    # 工作流状态定义
│       ├── nodes/              # 算子节点实现
│       │   ├── __init__.py
│       │   ├── node_factory.py # 算子工厂，注册和管理
│       │   ├── sql_node.py     # SQL查询算子
│       │   ├── classify_node.py    # 分类算子
│       │   └── semantic_filter_node.py  # 语义过滤算子
│       ├── prompt/             # 提示词模板
│       │   └── planner_prompt.py   # 规划器提示词
│       ├── chain.py            # LLM链定义
│       └── util.py             # 工作流工具函数
│
├── services/                   # 服务层
│   ├── __init__.py
│   ├── util.py                 # 服务工具函数
│   ├── datasource/             # 数据源服务
│   ├── knowledge/              # 知识库服务
│   ├── query/                  # 查询服务
│   ├── report/                 # 报告服务
│   └── workflow/               # 工作流服务
│
├── models/                     # 模型层
│   └── langchain_models.py     # LangChain模型配置
│
├── test/                       # 测试文件
│   └── workflow/
│       └── test_workflow.py    # 工作流测试
│
├── main.py                     # 程序入口
├── requirement.txt             # 依赖包列表
├── schema.txt                  # 数据库Schema定义
└── README.md                   # 项目说明文档
```

## 模块说明

### 1. API 层 (`api/`)
- **admin.py**: 提供管理后台相关的 API 接口

### 2. 配置层 (`config/`)
- **config.py**: 应用程序的全局配置参数
  - 数据库配置
  - API 配置
  - 日志配置
  - 各模块配置项（数据源、知识库、查询、报告、工作流）
  - Schema 文件路径配置
- **logger.py**: 日志系统的配置和初始化

### 3. 核心业务层 (`DataAgent/`)
核心业务逻辑的实现，按功能模块划分：

#### 3.1 数据源模块 (`datasource/`)
- **数据库连接** (`db_config_read.py`)
  - 支持多种数据库类型：MySQL、PostgreSQL、达梦（DM）
  - 从配置文件读取数据库连接信息
  - 统一的 SQLDatabase 实例创建

- **Schema 获取与解析** (`schema_obtain.py`, `schema_parse.py`)
  - `schema_obtain`: 批量获取表的原始 Schema（CREATE TABLE 语句）
  - `schema_parse`: 解析单个 CREATE TABLE 语句，提取表名、表注释、字段信息、样例数据
  - 返回结构化的表信息字典

- **Schema 增强** (`schema_enhance.py`) ⭐
  - **枚举值增强** (`schema_enum_enhance`)
    - 自动检测字符串类型的枚举字段
    - 批量查询 TOP10 最常见的枚举值
    - 智能合并原注释中已存在的枚举值（避免重复）
    - 区分完整枚举和部分枚举
    - 支持多数据库（MySQL、PostgreSQL、达梦）

  - **表描述生成** (`schema_table_description_enhance`)
    - 使用 LangChain 的 `chain.batch()` 批量生成表描述
    - 返回 `{表名: 表描述}` 格式

- **LangChain 集成** (`chain.py`)
  - 定义 LLM 调用链
  - 表描述生成的 Prompt 模板

- **Prompt 模板** (`prompt/`)
  - 存放各类 LLM 提示词模板
  - Schema 解析、枚举值提取等相关 Prompt


#### 3.2 知识库模块 (`knowledge/`)
- 知识库的构建和维护
- 知识的存储、检索和更新

#### 3.3 查询模块 (`query/`)
- 智能查询处理
- 自然语言到查询语句的转换

#### 3.4 报告模块 (`report/`)
- 报告模板管理
- 报告生成和导出

#### 3.5 工作流模块 (`workflow/`) ⭐ **核心模块**

**自然语言到工作流 (`nl2flow/`)**:
- **nl_parser.py**: 自然语言解析器
  - 将用户的自然语言查询解析为工作流节点列表
  - 调用 LLM 进行查询理解和算子规划
- **workflow_builder.py**: 工作流构建器
  - 基于 LangGraph 的 StateGraph 构建工作流
  - 支持节点添加、边连接、自动流水线连接
  - 提供从自然语言创建工作流的便捷接口
- **workflow_state.py**: 工作流状态定义
  - 基于 Pydantic 的状态模型
  - 跟踪查询、节点指令、执行结果等信息

**算子节点 (`nodes/`)**:
- **node_factory.py**: 算子工厂
  - 装饰器注册机制
  - 算子的发现和管理
- **sql_node.py**: SQL 查询算子
  - 处理结构化数据查询
- **classify_node.py**: 分类算子
  - 对文本进行端到端语义分类
- **semantic_filter_node.py**: 语义过滤算子
  - 基于语义相似度进行文本过滤

**提示词 (`prompt/`)**:
- **planner_prompt.py**: 规划器提示词模板
  - 定义算子编排规则
  - 指导 LLM 进行查询分解和算子选择

**LLM 链 (`chain.py`)**:
- **planner_chain**: 使用 LangChain 创建的 LLM 调用链
  - 结合提示词模板和 LLM 模型
  - 用于自然语言查询解析

### 4. 服务层 (`services/`)
- 提供具体的业务服务实现
- 与核心业务模块对应，包含各领域的服务逻辑

### 5. 模型层 (`models/`)
- **langchain_models.py**: LangChain 模型配置
  - LLM 模型初始化（Qwen、Coder等）
  - Embedding 模型配置（BGE等）
  - 模型 API 密钥和端点配置

### 6. 测试层 (`test/`)
- **test_workflow.py**: 工作流模块测试
  - 节点工厂测试
  - 手动工作流构建测试
  - 自然语言到工作流测试

## 核心功能

### 1. Schema 增强系统

#### 1.1 枚举值增强

自动识别字符串类型的枚举字段，并从数据库中提取 TOP10 最常见的值追加到字段注释中。

**功能特点**:
- ✅ 批量查询：一次查询获取所有字符串字段的枚举值
- ✅ 智能合并：自动去重，避免与原注释中的枚举值重复
- ✅ 完整性判断：区分"完整枚举"和"部分枚举"
- ✅ 多数据库支持：MySQL、PostgreSQL、达梦
- ✅ 性能优化：采样 10000 行，避免全表扫描

**使用示例**:
```python
from DataAgent.datasource.schema_obtain import schema_obtain
from DataAgent.datasource.schema_enhance import schema_enum_enhance
from langchain_community.utilities import SQLDatabase

# 1. 连接数据库
uri = 'mysql+mysqlconnector://user:password@host:port/database'
business_db = SQLDatabase.from_uri(uri)

# 2. 获取表的 Schema（原始 + 解析）
table_names = ['table1', 'table2', 'table3']
raw_schemas_by_table, parsed_schemas_by_table = schema_obtain(business_db, table_names)

# 3. 枚举值增强
enhanced_schemas = schema_enum_enhance(parsed_schemas_by_table, business_db)

# 4. 查看增强后的结果
for table_name, table_info in enhanced_schemas.items():
    print(f"表名: {table_name}")
    for column in table_info['columns']:
        print(f"  字段: {column['name']}")
        print(f"  类型: {column['type']}")
        print(f"  注释: {column['comment']}")
        print()
```

**输出示例**:
```
字段: 工单类别
类型: varchar(50)
注释: 枚举类型，完整取值包括：['一般', '次紧急', '紧急']

字段: 二级分类
类型: text
注释: 枚举类型，常见值包括：['工商(消保)', '人力保障', '邮政通信' ...]

字段: 新一级分类
类型: text
注释: 枚举类型，常见值包括：['社会管理类', '住房和城乡建设类', ... '党政机关类'] ...
```

#### 1.2 表描述生成

使用 LangChain 批量调用大模型，自动生成表的业务描述。

**使用示例**:
```python
from DataAgent.datasource.schema_enhance import schema_table_description_enhance

# 批量生成表描述
generated_descriptions = schema_table_description_enhance(raw_schemas_by_table)

# 结果格式: {表名: 表描述}
for table_name, description in generated_descriptions.items():
    print(f"{table_name}: {description}")
```

#### 1.3 核心函数说明

**`schema_enum_enhance(parsed_schemas_by_table, business_db)`**
- 输入: 解析后的表信息字典、SQLDatabase 实例
- 输出: 增强后的表信息字典（枚举值已追加到字段注释）
- 参数:
  - `sample_rows`: 采样行数（默认 10000）
  - `top_n`: 返回的枚举值数量（默认 10）
  - `max_distinct_threshold`: 枚举类型判断阈值（默认 100）

**`schema_table_description_enhance(raw_schemas_by_table)`**
- 输入: {表名: CREATE TABLE 语句}
- 输出: {表名: 表描述}
- 使用 LangChain 的 `chain.batch()` 批量调用

### 2. 自然语言查询解析
系统接收用户的自然语言查询，通过 LLM 理解意图，自动生成可执行的工作流算子序列。

**支持的算子类型**:
- `sql`: 结构化数据查询、过滤、聚合
- `classify`: 对文本进行语义分类
- `semantic_filter`: 基于语义相似度过滤文本
- `ner`: 命名实体识别
- `mask`: 敏感信息脱敏
- `cluster`: 文本聚类
- `summarize`: 文本摘要
- `search`: 语义相似度搜索

### 2. 工作流编排
基于 LangGraph 的异步工作流引擎：
- 声明式工作流定义
- 自动节点连接
- 状态管理和传递
- 异步执行支持

### 3. 算子扩展机制
通过装饰器轻松注册新算子：
```python
@register_node(
    name="my_operator",
    description="我的自定义算子",
)
async def my_operator(state: WorkflowState) -> WorkflowState:
    # 算子逻辑
    return state
```

## 安装与运行

### 环境要求
- Python 3.8+
- 依赖包见 `requirement.txt`

### 安装依赖
```bash
pip install -r requirement.txt
```

### 运行测试
```bash
# 测试工作流模块
python test/workflow/test_workflow.py
```

### 运行程序
```bash
python main.py
```

## 配置说明

### 数据库配置
在 `config/config.py` 中配置数据库连接信息，支持环境变量覆盖。

### Schema 配置
在 `schema.txt` 中定义数据库表结构，用于 LLM 理解数据模型。

### 语义字段配置
在 `config/config.py` 中配置 `SEMANTIC_FIELDS`，指定需要进行语义分析的字段。

## 代码导入规范

项目使用绝对导入路径，所有模块应从项目根目录 `DataAgent` 开始导入：

```python
from DataAgent.workflow.nl2flow.nl_parser import parse_workflow
from DataAgent.workflow.nodes.node_factory import register_node
from DataAgent.workflow.chain import planner_chain
from config.config import schema, sementic_field
```

## 开发指南

### 添加新算子
1. 在 `DataAgent/workflow/nodes/` 下创建新文件（如 `my_operator.py`）
2. 使用 `@register_node` 装饰器注册算子
3. 实现异步函数，接收并返回 `WorkflowState`
4. 在 `nodes/__init__.py` 中导出新算子

### 修改工作流逻辑
1. 在 `nl2flow/` 目录下修改解析器或构建器
2. 更新 `prompt/planner_prompt.py` 中的提示词模板
3. 运行测试验证修改

## 技术栈

- **LLM 框架**: LangChain, LangGraph
- **模型**: Qwen (通义千问), BGE Embedding
- **异步处理**: asyncio
- **数据验证**: Pydantic
- **日志**: Python logging

## 许可证

[待添加]

## 贡献指南

[待添加]
