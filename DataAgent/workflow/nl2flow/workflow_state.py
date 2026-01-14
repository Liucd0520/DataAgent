from typing_extensions import TypedDict
from typing import Literal, List, Dict
from pydantic import BaseModel

class WorkflowState(BaseModel):
    """
    Represents the state of our graph.

    Attributes:
        query: query
        db_name: database name
        documents: list of documents
    """

    original_nl_query: str = ''
    sub_node_name: List = [] 
    sub_node_instruction: List = []
    sub_node_result: List = []
    sub_node_error: List = []
    current_node: str = ''
    


if __name__ == '__main__':
    state = WorkflowState()
    print(state)
    state.sub_node_instruction=['1', '2']
    print(state)
    state.sub_node_error.append('3')

    print(state)