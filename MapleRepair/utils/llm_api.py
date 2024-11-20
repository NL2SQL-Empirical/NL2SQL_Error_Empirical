import openai
from openai import OpenAI
from MapleRepair.config import openai_api_key, openai_base_url
from typing import Union, List, Tuple
from retry import retry
from pathlib import Path
from MapleRepair.config import default_model

openai_client = OpenAI(
    api_key=openai_api_key, 
    base_url=openai_base_url
)

@retry(tries=5, delay=3)
def gpt_request(prompt:str, model:str=default_model, temperature:float=0, use_json:bool=False, log_path:Path=None) -> Tuple[str, dict]:
    result = openai_client.chat.completions.create(
        messages = [
            {
                'role': 'user',
                'content': prompt,
            }
        ],
        model = model,
        temperature = temperature,
        response_format = { "type": "json_object" if use_json else "text"}
    )
    s_result = result.choices[0].message.content
    usage = result.usage.model_dump()
    
    if log_path:
        with open(log_path, 'a') as f:
            f.write("\n===== Prompt =====\n")
            f.write(prompt)
            f.write("\n===== LLM's Response =====\n")
            f.write(s_result)
            f.write("\n===== LLM End =====\n")
    
    return s_result, usage

def get_embedding(texts:List[str], model="text-embedding-3-small"):
   return openai_client.embeddings.create(input = texts, model=model).data
