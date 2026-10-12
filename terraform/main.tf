resource "azurerm_resource_group" "platform" {
  name     = local.resource_prefix
  location = var.location
  tags     = local.common_tags
}

# Keep the container image private; App Service pulls it with the app identity.
resource "azurerm_container_registry" "platform" {
  name                = local.registry_name
  resource_group_name = azurerm_resource_group.platform.name
  location            = azurerm_resource_group.platform.location
  sku                 = "Basic"
  admin_enabled       = false
  tags                = local.common_tags
}

resource "azurerm_service_plan" "platform" {
  name                = "${local.resource_prefix}-plan"
  resource_group_name = azurerm_resource_group.platform.name
  location            = var.app_service_location
  os_type             = "Linux"
  sku_name            = var.app_service_sku
  tags                = local.common_tags
}

resource "azurerm_log_analytics_workspace" "platform" {
  name                = "${local.resource_prefix}-logs"
  location            = azurerm_resource_group.platform.location
  resource_group_name = azurerm_resource_group.platform.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.common_tags
}

resource "azurerm_eventhub_namespace" "telemetry" {
  name                          = "${local.resource_prefix}-eh"
  location                      = azurerm_resource_group.platform.location
  resource_group_name           = azurerm_resource_group.platform.name
  sku                           = "Standard"
  capacity                      = 1
  auto_inflate_enabled          = true
  maximum_throughput_units      = 2
  minimum_tls_version           = "1.2"
  public_network_access_enabled = true
  tags                          = local.common_tags
}

resource "azurerm_eventhub" "telemetry" {
  name              = "telemetry-v1"
  namespace_id      = azurerm_eventhub_namespace.telemetry.id
  partition_count   = 4
  message_retention = 1
}

resource "azurerm_eventhub_consumer_group" "processors" {
  name                = "mission-telemetry-processors-v1"
  namespace_name      = azurerm_eventhub_namespace.telemetry.name
  eventhub_name       = azurerm_eventhub.telemetry.name
  resource_group_name = azurerm_resource_group.platform.name
}

resource "azurerm_eventhub_namespace_authorization_rule" "application" {
  name                = "${local.resource_prefix}-app"
  namespace_name      = azurerm_eventhub_namespace.telemetry.name
  resource_group_name = azurerm_resource_group.platform.name
  listen              = true
  send                = true
  manage              = false
}

resource "azurerm_key_vault" "platform" {
  name                          = "${substr(local.prefix, 0, 12)}-${random_string.suffix.result}-kv"
  location                      = azurerm_resource_group.platform.location
  resource_group_name           = azurerm_resource_group.platform.name
  tenant_id                     = data.azurerm_client_config.current.tenant_id
  sku_name                      = "standard"
  rbac_authorization_enabled    = true
  purge_protection_enabled      = true
  soft_delete_retention_days    = 7
  public_network_access_enabled = true
  tags                          = local.common_tags
}

resource "azurerm_user_assigned_identity" "api" {
  name                = "${local.resource_prefix}-identity"
  location            = azurerm_resource_group.platform.location
  resource_group_name = azurerm_resource_group.platform.name
  tags                = local.common_tags
}

# Unique demo credentials are generated in Terraform and stored in Key Vault.
resource "random_password" "jwt_secret" {
  length  = 48
  special = false
}

resource "random_password" "mission_password" {
  length  = 24
  special = false
}

resource "random_password" "scientist_password" {
  length  = 24
  special = false
}

resource "azurerm_role_assignment" "api_key_vault_reader" {
  scope                = azurerm_key_vault.platform.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.api.principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "api_acr_pull" {
  scope                = azurerm_container_registry.platform.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.api.principal_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "deployer_key_vault_secrets_officer" {
  scope                = azurerm_key_vault.platform.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
  principal_type       = "User"
}

resource "azurerm_key_vault_secret" "jwt_secret" {
  name         = "jwt-secret"
  value        = random_password.jwt_secret.result
  key_vault_id = azurerm_key_vault.platform.id
}

resource "azurerm_key_vault_secret" "mission_username" {
  name         = "mission-username"
  value        = var.mission_username
  key_vault_id = azurerm_key_vault.platform.id
}

resource "azurerm_key_vault_secret" "mission_password" {
  name         = "mission-password"
  value        = random_password.mission_password.result
  key_vault_id = azurerm_key_vault.platform.id
}

resource "azurerm_key_vault_secret" "scientist_username" {
  name         = "scientist-username"
  value        = var.scientist_username
  key_vault_id = azurerm_key_vault.platform.id
}

resource "azurerm_key_vault_secret" "scientist_password" {
  name         = "scientist-password"
  value        = random_password.scientist_password.result
  key_vault_id = azurerm_key_vault.platform.id
}

resource "azurerm_key_vault_secret" "kafka_sasl_username" {
  name         = "kafka-sasl-username"
  value        = "$ConnectionString"
  key_vault_id = azurerm_key_vault.platform.id
}

resource "azurerm_key_vault_secret" "kafka_sasl_password" {
  name         = "kafka-sasl-password"
  value        = azurerm_eventhub_namespace_authorization_rule.application.primary_connection_string
  key_vault_id = azurerm_key_vault.platform.id
}

resource "azurerm_linux_web_app" "api" {
  name                            = "${local.resource_prefix}-api"
  resource_group_name             = azurerm_resource_group.platform.name
  location                        = var.app_service_location
  service_plan_id                 = azurerm_service_plan.platform.id
  https_only                      = true
  public_network_access_enabled   = true
  key_vault_reference_identity_id = azurerm_user_assigned_identity.api.id
  tags                            = local.common_tags

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.api.id]
  }

  site_config {
    always_on                                     = true
    ftps_state                                    = "Disabled"
    http2_enabled                                 = true
    minimum_tls_version                           = "1.2"
    health_check_path                             = "/health"
    health_check_eviction_time_in_min             = 2
    container_registry_use_managed_identity       = true
    container_registry_managed_identity_client_id = azurerm_user_assigned_identity.api.client_id

    application_stack {
      docker_image_name   = "${azurerm_container_registry.platform.login_server}/mission-telemetry:${var.image_tag}"
      docker_registry_url = "https://${azurerm_container_registry.platform.login_server}"
    }
  }

  app_settings = {
    APP_ENV                             = "demo"
    DEMO_MODE                           = "true"
    WEBSITES_PORT                       = "8000"
    WEBSITES_ENABLE_APP_SERVICE_STORAGE = "false"
    JWT_SECRET                          = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.jwt_secret.versionless_id})"
    MISSION_USERNAME                    = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.mission_username.versionless_id})"
    MISSION_PASSWORD                    = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.mission_password.versionless_id})"
    SCIENTIST_USERNAME                  = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.scientist_username.versionless_id})"
    SCIENTIST_PASSWORD                  = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.scientist_password.versionless_id})"
    KAFKA_BOOTSTRAP_SERVERS             = "${azurerm_eventhub_namespace.telemetry.name}.servicebus.windows.net:9093"
    KAFKA_TOPIC                         = azurerm_eventhub.telemetry.name
    KAFKA_SASL_USERNAME                 = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.kafka_sasl_username.versionless_id})"
    KAFKA_SASL_PASSWORD                 = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.kafka_sasl_password.versionless_id})"
  }

  depends_on = [
    azurerm_role_assignment.api_key_vault_reader,
    azurerm_role_assignment.api_acr_pull,
  ]
}

resource "azurerm_monitor_diagnostic_setting" "api" {
  name                       = "${local.resource_prefix}-diagnostics"
  target_resource_id         = azurerm_linux_web_app.api.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.platform.id

  enabled_log {
    category = "AppServiceConsoleLogs"
  }

  enabled_log {
    category = "AppServiceHTTPLogs"
  }

  enabled_log {
    category = "AppServicePlatformLogs"
  }

  enabled_metric {
    category = "AllMetrics"
  }
}

resource "azurerm_consumption_budget_resource_group" "monthly" {
  name              = "${local.resource_prefix}-monthly-budget"
  resource_group_id = azurerm_resource_group.platform.id
  amount            = var.budget_amount_usd
  time_grain        = "Monthly"

  time_period {
    start_date = "2026-10-01T00:00:00Z"
    end_date   = "2036-10-01T00:00:00Z"
  }

  notification {
    enabled        = true
    threshold      = 50
    operator       = "GreaterThan"
    threshold_type = "Actual"
    contact_emails = [var.budget_contact_email]
  }

  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThan"
    threshold_type = "Actual"
    contact_emails = [var.budget_contact_email]
  }

  notification {
    enabled        = true
    threshold      = 100
    operator       = "GreaterThan"
    threshold_type = "Actual"
    contact_emails = [var.budget_contact_email]
  }
}
