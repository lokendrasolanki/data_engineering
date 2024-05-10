from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

# Authenticate with Azure
credential = DefaultAzureCredential()

# Create a Secret Client
key_vault_name = "<key-vault-name>"
key_vault_uri = f"https://{key_vault_name}.vault.azure.net"
secret_client = SecretClient(vault_url=key_vault_uri, credential=credential)

# Set a secret value
secret_name = "<secret-name>"
secret_value = "<secret-value>"
secret = secret_client.set_secret(secret_name, secret_value)
print(f"SecretValue: {secret.value}")


