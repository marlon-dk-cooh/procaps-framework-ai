# Databricks notebook source
import json
from dataclasses import asdict
from config.settings import settings
from src.steps.s00_read_files import classify_file_by_extension

# ======== CARGA DE SETTINGS =============
STEP_NAME = "s01_extract_ocr"

# 1. Definir conexiones
storage = StorageAccount(
    account_name=settings.azure_storage_account_name,
    account_key=settings.azure_storage_account_key
)

doc_intel = DocumentIntelligenceConnection(
    endpoint=settings.azure_document_intelligence_endpoint,
    key=settings.azure_document_intelligence_key
)

if __name__ == "__main__":
    main()
