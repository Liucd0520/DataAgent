from models.langchain_models import pro_llm
from langchain.prompts import PromptTemplate
from DataAgent.datasource.prompt.table_description_prompt import table_description_prompt_template

prompt = PromptTemplate(template=table_description_prompt_template, input_variables=["table_schema", ])
table_descpt_chain = prompt | pro_llm
