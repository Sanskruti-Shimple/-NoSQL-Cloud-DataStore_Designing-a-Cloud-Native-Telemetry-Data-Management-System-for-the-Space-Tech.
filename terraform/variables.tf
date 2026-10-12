variable "project_name" {
  description = "Short, lowercase project identifier used in Azure resource names."
  type        = string
  default     = "mission-telemetry"
}

variable "environment" {
  description = "Deployment environment label."
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region for the application foundation."
  type        = string
  default     = "westus"
}

variable "app_service_location" {
  description = "Azure region for the App Service plan and app; West US 2 is used because West US has no available plan quota."
  type        = string
  default     = "westus2"
}

variable "image_tag" {
  description = "Container image tag pushed to the project ACR before the web app is deployed."
  type        = string
  default     = "mission-v1"
}

variable "app_service_sku" {
  description = "Linux Functions Consumption plan SKU."
  type        = string
  default     = "Y1"

  validation {
    condition     = contains(["Y1"], var.app_service_sku)
    error_message = "Use a supported Linux App Service SKU."
  }
}

variable "budget_amount_usd" {
  description = "Monthly resource-group budget notification threshold in USD; not a spending cap."
  type        = number
  default     = 100

  validation {
    condition     = var.budget_amount_usd > 0
    error_message = "budget_amount_usd must be greater than zero."
  }
}

variable "budget_contact_email" {
  description = "Email address for monthly Azure budget threshold notifications."
  type        = string
}

variable "mission_username" {
  description = "Mission-control demo account name."
  type        = string
  default     = "mission-operator"
}

variable "scientist_username" {
  description = "Scientist demo account name."
  type        = string
  default     = "scientist"
}
