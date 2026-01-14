"""
Workflow Builder - 工作流构建器
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
基于State的工作流构建器,不需要pre/post工具
"""
from __future__ import annotations
import sys
import os
import asyncio
from typing import Callable, Dict, List, Tuple
from langgraph.graph import StateGraph, END, START

# 添加项目根目录到sys.path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, project_root)

from DataAgent.workflow.nl2flow.nl_parser import parse_workflow
from DataAgent.workflow.nodes.node_factory import NodeFactory

class WorkflowBuilder:
    """
    工作流构建器

    功能:
    1. 添加节点到工作流
    2. 自动连接节点形成Pipeline
    3. 编译为LangGraph可执行的图
    """

    def __init__(self, state_model: type, entry_point: str = "start"):
        """
        初始化构建器

        Args:
            state_model: 状态模型类
            entry_point: 入口节点名称
        """
        self.state_model = state_model
        self.entry_point = entry_point
        self.nodes: Dict[str, Tuple[Callable, str]] = {}  # {name: (func, name)}
        self.edges: List[tuple] = []

    def add_node(self,
                 name: str,
                 func: Callable) -> 'WorkflowBuilder':
        """
        添加节点

        Args:
            name: 节点名称
            func: 节点函数

        Returns:
            self (支持链式调用)

        Example:
            builder.add_node("drop_duplicates", drop_duplicates_node)
        """
        self.nodes[name] = (func, name)
        return self

    def add_nodes(self, nodes: List[Tuple[str, Callable]]) -> 'WorkflowBuilder':
        """
        批量添加节点

        Args:
            nodes: 节点列表,每个元素是 (name, func) 元组

        Returns:
            self
        """
        for name, func in nodes:
            self.add_node(name, func)
        return self

    def add_edge(self, src: str, dst: str) -> 'WorkflowBuilder':
        """
        添加边(连接两个节点)

        Args:
            src: 源节点
            dst: 目标节点

        Returns:
            self
        """
        self.edges.append((src, dst))
        return self

    def add_edges(self, edges: List[tuple]) -> 'WorkflowBuilder':
        """
        批量添加边

        Args:
            edges: 边列表 [(src1, dst1), (src2, dst2), ...]

        Returns:
            self
        """
        self.edges.extend(edges)
        return self

    def auto_connect(self) -> 'WorkflowBuilder':
        """
        自动连接节点形成Pipeline

        按照节点添加的顺序依次连接,形成线性Pipeline

        Returns:
            self
        """
        node_names = list(self.nodes.keys())

        for i in range(len(node_names) - 1):
            self.edges.append((node_names[i], node_names[i + 1]))

        return self

    def _wrap_node_function(self, func: Callable, name: str) -> Callable:
        """
        包装节点函数

        Args:
            func: 节点函数
            name: 节点唯一名称 (如 sql_0, sql_1)

        Returns:
            包装后的异步函数
        """
        async def wrapped_func(state):
            print(f"[Workflow] Executing node: {name}")

            # 设置当前节点名称
            state.current_node = name

            # 如果是异步函数
            if asyncio.iscoroutinefunction(func):
                result_state = await func(state)
            else:
                result_state = func(state)

            return result_state

        return wrapped_func

    def build(self):
        """
        构建并返回编译后的LangGraph图

        Returns:
            编译后的StateGraph
        """
        sg = StateGraph(self.state_model)

        # 添加节点
        for name, (func, _) in self.nodes.items():
            print('===>', name, func)
            wrapped_func = self._wrap_node_function(func, name)
            sg.add_node(name, wrapped_func)

        # 设置入口点：将START连接到第一个节点
        if self.nodes:
            first_node = list(self.nodes.keys())[0]
            sg.set_entry_point(first_node)

        # 添加普通边
        for src, dst in self.edges:
            if dst == "_end_":
                sg.add_edge(src, END)
            else:
                sg.add_edge(src, dst)

        return sg.compile()

    def visualize(self, output_path: str = None):
        """
        可视化工作流图

        Args:
            output_path: 输出文件路径(可选)
        """
        try:
            graph = self.build()

            # 输出文本表示
            print("\nWorkflow Structure:")
            print("=" * 60)
            for name in self.nodes.keys():
                print(f"  [{name}]")
            print("\nEdges:")
            for src, dst in self.edges:
                print(f"  {src} -> {dst}")
            print("=" * 60)

        except Exception as e:
            print(f"[Error] Visualization failed: {e}")


class WorkflowFromNL:
    """
    从自然语言创建工作流的便捷类

    结合LLMParser和WorkflowBuilder
    """

    @staticmethod
    def create_from_query(query: str, state: type) -> WorkflowBuilder:
        """
        从自然语言查询创建工作流

        Args:
            query: 自然语言查询
            state: 状态机

        Returns:
            WorkflowBuilder实例

        Example:
            builder = WorkflowFromNL.create_from_query(
                "Remove duplicates and drop nulls",
                WorkflowState
            )
            workflow = builder.build()
        """
        

        nodes_pipeline = parse_workflow(query)

        if not nodes_pipeline:
            print(f"No nodes found for query: {query}")
            return WorkflowBuilder(state), nodes_pipeline

        # 创建构建器
        builder = WorkflowBuilder(state, entry_point="")  # 先设置为空，稍后更新

        # 添加节点
        node_name_counts = {}  # 跟踪每个节点类型出现的次数，用于生成唯一名称
        for node in nodes_pipeline:
            op_type = node['op']

            # 生成唯一节点名称: 操作类型_索引 (如 sql_0, sql_1, classify_0)
            if op_type not in node_name_counts:
                node_name_counts[op_type] = 0
            unique_node_name = f"{op_type}_{node_name_counts[op_type]}"
            node_name_counts[op_type] += 1

            # 更新node中的op为唯一名称，这样后面设置到state时就是带编号的
            node['op'] = unique_node_name

            # 从NodeFactory获取节点函数 (使用原始名称)
            metadata = NodeFactory.get_node(op_type)
            if metadata:
                builder.add_node(
                    name=unique_node_name,
                    func=metadata.func
                )
            else:
                print(f"[Warning] Node {op_type} not found in registry")

        # 自动连接节点（按添加顺序）
        builder.auto_connect()

        return builder, nodes_pipeline


# 便捷函数
def create_workflow(state: type,
                   entry_point: str = "start") -> WorkflowBuilder:
    """
    创建工作流构建器

    Args:
        state_model: 状态模型类
        entry_point: 入口节点名称

    Returns:
        WorkflowBuilder实例

    Example:
        builder = create_workflow(WorkflowState, "start")
        builder.add_node("process", process_node).auto_connect()
        workflow = builder.build()
    """
    return WorkflowBuilder(state, entry_point)


def create_workflow_from_nl(query: str, state: type):
    """
    从自然语言查询创建工作流

    Args:
        query: 自然语言查询
        state_model: 状态模型类

    Returns:
        编译后的工作流

    Example:
        workflow = create_workflow_from_nl(
            "Remove duplicates and save to output.csv",
            WorkflowState
        )
        result = await workflow.ainvoke(initial_state)
    """
    builder, nodes_pipeline = WorkflowFromNL.create_from_query(query, state)
    return builder.build(), nodes_pipeline
