# Databricks notebook source
from src.core.file_helpers import classify_file_by_extension, group_files_by_extension, FILE_GROUPS

# ======== CARGA DE SETTINGS =============
STEP_NAME = "s00_read_files"
