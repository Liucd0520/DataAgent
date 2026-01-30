
from langchain_openai import  ChatOpenAI
from openai import OpenAI
import numpy as np 
from langchain.prompts import PromptTemplate

# 最终从配置文件里读取模型信息
pro_llm = ChatOpenAI(model= "qwen3-max-2025-09-23", 
                    base_url='https://dashscope.aliyuncs.com/compatible-mode/v1',  
                    api_key='sk-4e27d583808849128b07458af74724a6',
                    temperature=0,
                    )

coder32b_llm = ChatOpenAI(model=  "Qwen2.5-Coder-32B-Instruct",
                    base_url='http://172.31.24.112:33080/v1', 
                    api_key='yfzx202510',
                    temperature=0,
                    )


# add 7b or 14b model 




openai_api_key_emb = "EMPTY"
openai_api_base_emb = 'http://172.31.24.111:8003/v1'  #  'http://172.31.24.23:8002/v1' # 'http://192.168.0.11:8076/v1' 
client_emb = OpenAI(api_key=openai_api_key_emb,
                base_url=openai_api_base_emb
                )

def embedding_bge(query_list):
    
    responses = client_emb.embeddings.create(
        input=query_list,
        model='bge-large-embedding',
    )
    embedding_list = [output_data.embedding for output_data in  responses.data]
    
    return np.array(embedding_list)

if __name__ == '__main__':
    print(pro_llm.invoke('hello'))