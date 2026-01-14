"""
Node Factory - 算子节点注册工厂
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
提供节点注册、发现和创建功能
"""
from __future__ import annotations
from typing import Callable, Dict, List, Optional, Any
from dataclasses import dataclass
import json
from pathlib import Path


@dataclass
class NodeMetadata:
    """节点元数据"""
    name: str                           # 节点名称
    description: str                    # 节点描述
    func: Callable = None               # 节点函数


class NodeFactory:
    """
    节点工厂类 - 管理所有算子节点的注册和创建
    """
    _registry: Dict[str, NodeMetadata] = {}

    @classmethod
    def register(cls,
                 name: str,
                 description: str,
                 ):
        """
        装饰器: 注册一个算子节点

        Args:
            name: 节点名称
            description: 节点功能描述
        """

        def decorator(func: Callable):
            metadata = NodeMetadata(
                name=name,
                description=description,
                func=func,  # 保存函数引用
            )
            if name in cls._registry:
                raise ValueError(f"Node '{name}' already registered")

            cls._registry[name] = metadata
            func._node_metadata = metadata  # 将元数据附加到函数上
            return func

        return decorator

    @classmethod
    def get_node(cls, name: str) -> Optional[NodeMetadata]:
        """获取指定名称的节点元数据"""
        return cls._registry.get(name)

    @classmethod
    def get_all_nodes(cls) -> Dict[str, NodeMetadata]:
        """获取所有已注册的节点"""
        return dict(cls._registry)

    @classmethod
    def list_all_nodes(cls) -> List[Dict[str, Any]]:
        """
        列出所有节点的基本信息(用于展示)

        Returns:
            节点信息列表,每个元素包含 name, description
        """
        return [
            {
                "name": name,
                "description": metadata.description,
            }
            for name, metadata in sorted(cls._registry.items())
        ]

    
# 便捷函数
def register_node(name: str,
                  description: str,
                ):
    
    """
    节点注册装饰器的便捷函数

    Example:
        @register_node(
            name="sql_node",
            description="查询数据库中的结构化数据",
        )
        def sql_node(state, **kwargs):
            pass
    """
    return NodeFactory.register(
        name=name,
        description=description,
    )
