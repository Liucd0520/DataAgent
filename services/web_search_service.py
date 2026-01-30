"""
联网搜索服务层
"""

from typing import Dict, Any
from DataAgent.knowledge.web_search import web_search_wrapper


def search_term_explanation(term: str, count: int = 3) -> Dict[str, Any]:
    """
    搜索术语解释

    Args:
        term: 要搜索的术语
        count: 返回的搜索结果数量（默认3）

    Returns:
        Dict[str, Any]: 包含 success 和 explanation 的字典
    """
    try:
        # 调用底层联网搜索函数
        web_summary_list = web_search_wrapper(query=term, count=count)

        if not web_summary_list:
            return {
                "success": False,
                "explanation": f"未找到关于 '{term}' 的相关信息"
            }

        # 将多个搜索结果合并成一个解释
        # 如果只有一个结果，直接使用；如果有多个，用换行符连接
        if len(web_summary_list) == 1:
            explanation = web_summary_list[0]
        else:
            # 将多个摘要用换行符连接，形成更详细的解释
            explanation = "\n\n".join([f"• {summary}" for summary in web_summary_list])

        return {
            "success": True,
            "explanation": explanation
        }

    except Exception as e:
        return {
            "success": False,
            "explanation": f"搜索失败: {str(e)}"
        }
