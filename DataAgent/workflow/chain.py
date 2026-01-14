from models.langchain_models import pro_llm
from langchain.prompts import PromptTemplate
from DataAgent.workflow.prompt.planner_prompt import planner_prompt_template

prompt = PromptTemplate(template=planner_prompt_template, input_variables=["query", "schema", "sementic_field"])
planner_chain = prompt | pro_llm
