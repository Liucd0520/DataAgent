

import sys
import os
import pandas as pd

# 添加项目根目录到sys.path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from DataAgent.workflow.nl2flow.workflow_state import WorkflowState
from DataAgent.workflow.nl2flow.workflow_builder import create_workflow, create_workflow_from_nl
from DataAgent.workflow.nodes import *



def test_node_factory():
    """测试节点工厂"""
    print("\n" + "=" * 60)
    print("TEST 1: Node Factory")
    print("=" * 60)

    nodes = NodeFactory.get_all_nodes()
    print(f"✓ Registered {len(nodes)} operators")
    print(f"show nodes: {nodes}")


def test_manual_workflow():
    """测试手动工作流构建"""
    print("\n" + "=" * 60)
    print("TEST 2: Manual Workflow Builder")
    print("=" * 60)



    # 创建状态
    state = WorkflowState()
    state.original_nl_query = '过去半年道路积水的工单有多少'
    state.sub_node_name = ['sql', 'classify', 'sql']
    state.sub_node_instruction = ['过滤半年内的“内容描述”列',  "判断“内容描述”列的文本是“道路积水”事件吗", '联合“是”道路积水的列和原表统计数量']


    # 构建工作流
    builder = create_workflow(WorkflowState, entry_point='sql')
    builder.add_node("sql", sql)
    builder.add_node("classify", classify)
    builder.auto_connect()

    print("✓ Built workflow with 2 nodes")


    workflow = builder.build()
    import asyncio
    final_state = asyncio.run(workflow.ainvoke(state))
    
    print(f"✓ Executed workflow")
    print(f"  - Queries: {state.original_nl_query}")
    print(f"  - Outputs: {final_state}")
    print(f"  - Errors: {state.sub_node_error}")



def test_workflow_from_nl():
    """测试从自然语言创建工作流"""
    print("\n" + "=" * 60)
    print("TEST 3: Workflow from Natural Language")
    print("=" * 60)

    # query = "Remove duplicates"
    query = '过去半年道路积水的工单有多少'
    print(f"✓ Query: '{query}'")

   # 创建状态实例
    state = WorkflowState()
    
    # 创建工作流
    workflow, nodes_pipeline = create_workflow_from_nl(query, WorkflowState)
    
    # 更新状态
    sub_node_name_list = [each_node['op'] for each_node in nodes_pipeline]
    sub_node_instruction_list = [each_node['instruction'] for each_node in nodes_pipeline]
    
    state.original_nl_query = query 
    state.sub_node_name = sub_node_name_list
    state.sub_node_instruction = sub_node_instruction_list

    import asyncio
    final_state = asyncio.run(workflow.ainvoke(state))

    print(f"✓ Executed workflow")
    print(f"  - query: {query}")
    print(f"  - Output: {final_state}")



def run_all_tests():
    """运行所有测试"""
    print("\n" + "=" * 60)
    print("NEWPROJECT - TEST SUITE")
    print("=" * 60)

    tests = [
        # test_node_factory,
        # test_manual_workflow,

        # test_nl_parser,
        test_workflow_from_nl,
        # test_state_management,
        # test_parameterized_nodes
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"\n✗ Test failed: {test.__name__}")
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()

    # 总结
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Passed: {passed}/{len(tests)}")
    print(f"Failed: {failed}/{len(tests)}")

    if failed == 0:
        print("\n✓ All tests passed!")
        return 0
    else:
        print(f"\n✗ {failed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())
