output "resource_group_name" {
  description = "Resource group containing the telemetry application foundation."
  value       = azurerm_resource_group.platform.name
}

output "api_fqdn" {
  description = "Public HTTPS hostname; restrict ingress and configure a custom domain before production."
  value       = azurerm_linux_web_app.api.default_hostname
}

output "api_url" {
  description = "Public HTTPS URL for the deployed API."
  value       = "https://${azurerm_linux_web_app.api.default_hostname}"
}

output "container_registry_name" {
  description = "Project-owned Azure Container Registry used by the web app."
  value       = azurerm_container_registry.platform.name
}

output "key_vault_name" {
  description = "Key Vault containing runtime secrets."
  value       = azurerm_key_vault.platform.name
}

output "eventhub_namespace" {
  description = "Kafka-compatible Event Hubs namespace."
  value       = azurerm_eventhub_namespace.telemetry.name
}

output "log_analytics_workspace_id" {
  description = "Log Analytics workspace resource ID."
  value       = azurerm_log_analytics_workspace.platform.id
}

output "mission_control_demo_password" {
  description = "Generated mission-control demo login password. Treat as a secret."
  value       = random_password.mission_password.result
  sensitive   = true
}

output "scientist_demo_password" {
  description = "Generated scientist demo login password. Treat as a secret."
  value       = random_password.scientist_password.result
  sensitive   = true
}
