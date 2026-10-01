variable "project_id" {
  description = "GCP project id."
  type        = string
}

variable "region" {
  description = "Region for every resource. europe-west6 (Zurich) - Binance blocks US IPs."
  type        = string
  default     = "europe-west6"
}

variable "zone" {
  description = "Zone for the producer VM."
  type        = string
  default     = "europe-west6-a"
}

variable "billing_account" {
  description = "Billing account id (XXXXXX-XXXXXX-XXXXXX) for the budget alert."
  type        = string
}

variable "budget_amount" {
  description = "Monthly budget in the billing account's currency."
  type        = number
  default     = 20
}

variable "symbols" {
  description = "Comma-separated Binance symbols (lowercase)."
  type        = string
  default     = "btcusdt,ethusdt"
}

variable "streams" {
  description = "Comma-separated Binance stream types."
  type        = string
  default     = "trade"
}

variable "producer_machine_type" {
  description = "Producer VM size. e2-micro is plenty for a handful of trade streams."
  type        = string
  default     = "e2-micro"
}

variable "partition_expiration_days" {
  description = "BigQuery partitions older than this are deleted automatically (cost control)."
  type        = number
  default     = 30
}

variable "name_prefix" {
  description = "Prefix for resource names."
  type        = string
  default     = "crypto"
}
