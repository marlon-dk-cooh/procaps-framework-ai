#!/bin/bash

# Configuration: Replace this with your actual Key Vault name
VAULT_NAME="azkvaprocapsdevservicios"
ENV_FILE=".env"

# Check if az CLI is logged in
if ! az account show > /dev/null 2>&1; then
    echo "Please run 'az login' first to authenticate with Azure."
    exit 1
fi

if [ ! -f "$ENV_FILE" ]; then
    echo "File $ENV_FILE not found!"
    exit 1
fi

echo "Pushing secrets from $ENV_FILE to Azure Key Vault: $VAULT_NAME"
echo "------------------------------------------------------------"

# Read the .env file line by line
while IFS= read -r line || [ -n "$line" ]; do
    # Skip empty lines and comments
    if [[ -z "$line" ]] || [[ "$line" == \#* ]]; then
        continue
    fi

    # Extract KEY and VALUE from KEY=VALUE or KEY="VALUE"
    # Using cut or sed to split by the first '='
    KEY=$(echo "$line" | cut -d '=' -f 1)
    
    # Get the value part, remove leading/trailing spaces, and remove optional surrounding quotes
    VALUE=$(echo "$line" | cut -d '=' -f 2- | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^"//' -e 's/"$//')
    
    # Convert KEY from UPPER_SNAKE_CASE to kebab-case
    # az keyvault secret names can only contain alphanumeric characters and dashes
    SECRET_NAME=$(echo "$KEY" | tr '[:upper:]' '[:lower:]' | tr '_' '-')

    echo "Pushing secret: $SECRET_NAME (from env variable $KEY)..."
    
    # Run the Azure CLI command to set the secret
    az keyvault secret set --vault-name "$VAULT_NAME" --name "$SECRET_NAME" --value "$VALUE" --output none
    
    if [ $? -eq 0 ]; then
        echo "✅ Successfully added $SECRET_NAME"
    else
        echo "❌ Failed to add $SECRET_NAME"
    fi

done < "$ENV_FILE"

echo "------------------------------------------------------------"
echo "Finished pushing secrets."
