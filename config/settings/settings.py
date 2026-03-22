from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Application settings.
    app_name: str = "Procaps Framework AI"
    app_version: str = "0.0.1"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    # Azure services.
    azure_document_intelligence_endpoint: str = ""
    azure_document_intelligence_key: str = ""
    azure_document_intelligence_model: str = ""
    azure_openai_endpoint: str = ""
    azure_openai_key: str = ""
    azure_openai_model: str = ""
    azure_openai_api_version: str = ""

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"
        

settings = Settings()