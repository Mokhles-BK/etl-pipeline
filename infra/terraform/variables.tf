variable "docker_host" {
  description = "How Terraform reaches the Docker daemon. The default is Docker Desktop on Windows; on Linux or macOS use unix:///var/run/docker.sock."
  type        = string
  default     = "npipe:////./pipe/docker_engine"
}

variable "postgres_version" {
  description = "Postgres major version (image tag)."
  type        = string
  default     = "16"
}

variable "container_name" {
  description = "Name of the Postgres container."
  type        = string
  default     = "etl-postgres"
}

variable "volume_name" {
  description = "Docker volume that stores the database files, so data survives container re-creation."
  type        = string
  default     = "etl-postgres-data"
}

variable "db_name" {
  description = "Database created on first start."
  type        = string
  default     = "etl"
}

variable "db_user" {
  description = "Database user created on first start."
  type        = string
  default     = "etluser"
}

variable "db_password" {
  description = "Password for db_user. No default on purpose: pass it with TF_VAR_db_password or terraform.tfvars (both are kept out of git)."
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.db_password) >= 8
    error_message = "db_password must be at least 8 characters."
  }
}

variable "host_port" {
  description = "Port on this machine. 5433 by default so it does not clash with a Postgres already running on 5432."
  type        = number
  default     = 5433
}
