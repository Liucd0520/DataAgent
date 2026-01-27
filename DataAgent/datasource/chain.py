from models.langchain_models import pro_llm
from langchain.prompts import PromptTemplate
from DataAgent.datasource.prompt.table_description_prompt import table_description_prompt_template
from DataAgent.datasource.prompt.field_name_translation import translate_prompt_template

table_description_prompt = PromptTemplate(template=table_description_prompt_template, input_variables=["table_schema", ])
table_descpt_chain = table_description_prompt | pro_llm

translate_prompt = PromptTemplate(template=translate_prompt_template, input_variables=["field_name", ])
translate_chain = translate_prompt | pro_llm
