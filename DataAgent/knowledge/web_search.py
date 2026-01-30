import sys
from pathlib import Path
# 将项目根目录添加到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


import requests
from config import config 

# BoCha AI Search Python SDK
# 参考链接https://bocha-ai.feishu.cn/wiki/AT9VwqsrQinss7k84LQcKJY6nDh  （在最下面）
import requests, json
from typing import Iterator

def bocha_ai_search(
    query: str,
    api_key: str,
    api_url: str = "https://api.bochaai.com/v1/ai-search",
    freshness: str = "noLimit",
    answer: bool = False,
    stream: bool = False,
    count: int = 10
):
    """ 博查AI搜索 """
    data = {
        "query": query,
        "freshness": freshness,
        "answer": answer,
        "stream": stream,
        "count": count
    }

    resp = requests.post(
        api_url,
        headers={"Authorization": f"Bearer {api_key}"},
        json=data,
        stream=stream
    )

   
    if resp.status_code == 200:
        return resp.json()
    else:
        return { "code": resp.code, "msg": "bocha ai search api error." }


def web_search_wrapper(query: str, count=1):
    BOCHA_API_KEY = config.BOCHA_API_KEY 
    BOCHA_API_URL =config.BOCHA_API_URL 
    response = bocha_ai_search(
        api_url=BOCHA_API_URL,
        api_key=BOCHA_API_KEY,
        query=query,
        freshness="noLimit",
        answer=False,
        stream=False,
        count = count
    )

    web_contents = json.loads(response['messages'][0]['content'])
    web_summary_list = [web_contents['value'][i]['summary'] for i in range(count)]
    
    return web_summary_list




if __name__ == "__main__":
    query = "2025年进博会日期范围"
    web_summary_list = web_search_wrapper(query, count=3)
    print("Web Summaries:")
    for summary in web_summary_list:
        print('==>', summary)
    