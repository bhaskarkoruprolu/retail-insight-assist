import os
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser


class LLMClient:
    def __init__(self, model="gpt-4o-mini", temperature=0):
        self.llm = ChatOpenAI(
            model=model,
            temperature=temperature,
            api_key=os.getenv("OPENAI_API_KEY")
        )
        self.parser = StrOutputParser()

    def generate(self, prompt: str) -> str:
        chain = self.llm | self.parser
        return chain.invoke(prompt)
