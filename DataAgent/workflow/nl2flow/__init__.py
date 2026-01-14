"""
NewProject Workflow Module - 新工作流系统
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
一个独立的、简化的工作流系统,支持自然语言查询和自动节点组合
"""
import importlib
import sys
import os
from pathlib import Path

# 添加项目根目录到sys.path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 导入核心组件
from DataAgent.workflow.nodes.node_factory import NodeFactory, NodeMetadata, register_node
from .nl_parser import parse_workflow
from .workflow_builder import WorkflowBuilder, WorkflowFromNL, create_workflow, create_workflow_from_nl


# 导入状态类
from .workflow_state import WorkflowState

__all__ = [
    # Node Factory
    "NodeFactory",
    "NodeMetadata",
    "register_node",

    # NL Parser
    "LLMWorkflowParser",
    "parse_workflow",

    # Builder
    "WorkflowBuilder",
    "WorkflowFromNL",
    "create_workflow",
    "create_workflow_from_nl",

    # State
    "WorkflowState",
    "WorkflowRequest",
]


# ========================================================================
# 管理接口函数
# ========================================================================

def list_available_operators():
    """
    列出所有可用的算子节点

    Returns:
        算子信息列表,每个元素包含 name, description, category, tags
    """
    return NodeFactory.list_all_nodes()


def get_operator_info(operator_name: str):
    """
    获取指定算子的详细信息

    Args:
        operator_name: 算子名称

    Returns:
        NodeMetadata对象或None
    """
    return NodeFactory.get_node(operator_name)


def export_operators_catalog(filepath: str = "operators_catalog.json"):
    """
    导出算子目录到JSON文件

    Args:
        filepath: 导出文件路径
    """
    NodeFactory.export_registry_to_file(filepath)
    print(f"✓ Operators catalog exported to: {filepath}")
    print(f"✓ Total operators: {len(NodeFactory.get_all_nodes())}")


def search_operators(query: str):
    """
    搜索算子

    Args:
        query: 搜索关键词

    Returns:
        匹配的算子字典
    """
    return NodeFactory.search_nodes(query)


def print_operators_catalog():
    """打印所有可用算子的目录"""
    operators = list_available_operators()

    if not operators:
        print("No operators registered.")
        return

    print("\n" + "=" * 80)
    print("AVAILABLE OPERATORS CATALOG")
    print("=" * 80)

    # 按类别分组
    by_category = {}
    for op in operators:
        category = op['category']
        if category not in by_category:
            by_category[category] = []
        by_category[category].append(op)

    # 打印每个类别
    for category, ops in sorted(by_category.items()):
        print(f"\n[{category.upper()}]")
        print("-" * 80)
        for op in ops:
            print(f"  • {op['name']}")
            print(f"    Description: {op['description']}")
            if op['tags']:
                print(f"    Tags: {', '.join(op['tags'])}")
            print()

    print("=" * 80)
    print(f"Total: {len(operators)} operators\n")


# ========================================================================
# 自动加载算子节点
# ========================================================================
def _auto_load_operators():
    """自动加载所有算子节点"""
    current_dir = Path(__file__).resolve().parent

    # 查找所有Python文件(除了 __ 开头的)
    for py_file in current_dir.glob("*.py"):
        # 跳过 __init__.py 和 __ 开头的文件
        if py_file.name.startswith("__"):
            continue

        # 动态导入模块
        module_name = f"{__name__}.{py_file.stem}"
        try:
            importlib.import_module(module_name)
            print(f"[AutoLoad] Loaded operator module: {py_file.name}")
        except Exception as e:
            print(f"[Warning] Failed to import {module_name}: {e}")


# 自动加载算子节点
_auto_load_operators()
