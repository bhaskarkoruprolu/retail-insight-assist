"""from dotenv import load_dotenv
load_dotenv()

from langchain_openai import ChatOpenAI

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
print(llm.invoke("Say hello in one word"))
"""

"""import os
from dotenv import load_dotenv

load_dotenv()

print("API KEY FOUND:", os.getenv("OPENAI_API_KEY") is not None)
print("API KEY PREVIEW:", os.getenv("OPENAI_API_KEY")[:10])
"""

from llm_client import LLMClient

llm = LLMClient()
print(llm.generate("Say hello in one word"))
