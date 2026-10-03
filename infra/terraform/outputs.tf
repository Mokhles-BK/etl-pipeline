output "connection" {
  description = "Where the database is reachable from this machine."
  value = {
    host     = "127.0.0.1"
    port     = var.host_port
    database = var.db_name
    user     = var.db_user
  }
}

output "env_file" {
  description = "Ready-to-paste .env lines for the pipeline. Run: terraform output -raw env_file"
  sensitive   = true
  value = join("\n", [
    "DB_HOST=127.0.0.1",
    "DB_PORT=${var.host_port}",
    "DB_NAME=${var.db_name}",
    "DB_USER=${var.db_user}",
    "DB_PASSWORD=${var.db_password}",
  ])
}
