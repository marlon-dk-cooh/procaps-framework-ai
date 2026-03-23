from pydantic import BaseModel
from typing import Optional
from config.settings import settings
from src.utils.app_logger import get_logger
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_azure_ai.chat_models import AzureAIOpenAIApiChatModel
from azure.identity import DefaultAzureCredential

class LLMClient:
    def __init__(self):
        self.logger = get_logger(__name__)
        self.llm_enable = settings.llm_enabled
        self.client: Optional[AzureAIOpenAIApiChatModel] = None
        if self.llm_enable:
            self.client = AzureAIOpenAIApiChatModel(
                project_endpoint=settings.azure_openai_endpoint,
                credential=DefaultAzureCredential(),
                model=settings.azure_openai_model,
                api_version=settings.azure_openai_api_version,
                temperature=0.2, # #TODO: Adjust it
            )
            self.logger.info("LLM inicializado | deployment=%s", settings.azure_openai_model)
        else:
            self.logger.info("LLM deshabilitado | faltan credenciales o libreria langchain-openai")

    async def invoke(self, system_prompt: str, user_prompt: str):
        if not self.llm_enable or not self.client:
            self.logger.debug("LLM omitido | Cliente no disponible")
            return None
        try:
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt)
            ]
        
            response = await self.client.ainvoke(messages)
            self.logger.debug("LLM invocado exitosamente")
            return response.content[0]["text"]
        except Exception as e:
            self.logger.error("Error invocando LLM: %s", e)
            
            