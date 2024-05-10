from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Authenticate with Azure
credential = DefaultAzureCredential()

# Create a Secret Client
key_vault_name = "<key-vault-name>"
key_vault_uri = f"https://{key_vault_name}.vault.azure.net"
secret_client = SecretClient(vault_url=key_vault_uri, credential=credential)

# Get a secret value
secret_name = "<secret-name>"
secret = secret_client.get_secret(secret_name)
secret_value = secret.value
print(f"Secret Value: {secret_value}")
