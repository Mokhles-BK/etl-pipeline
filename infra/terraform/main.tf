# A local Postgres for the ETL pipeline, managed as code.
# Data lives in a named volume, so re-applying is repeatable. `terraform destroy`
# removes the container AND the volume: a clean slate.

resource "docker_image" "postgres" {
  name         = "postgres:${var.postgres_version}"
  keep_locally = true
}

resource "docker_volume" "pgdata" {
  name = var.volume_name
}

resource "docker_container" "postgres" {
  name     = var.container_name
  image    = docker_image.postgres.image_id
  restart  = "unless-stopped"
  must_run = true

  env = [
    "POSTGRES_DB=${var.db_name}",
    "POSTGRES_USER=${var.db_user}",
    "POSTGRES_PASSWORD=${var.db_password}",
  ]

  ports {
    internal = 5432
    external = var.host_port
    ip       = "127.0.0.1" # reachable from this machine only, never the network
  }

  volumes {
    volume_name    = docker_volume.pgdata.name
    container_path = "/var/lib/postgresql/data"
  }

  healthcheck {
    test     = ["CMD-SHELL", "pg_isready -U ${var.db_user} -d ${var.db_name}"]
    interval = "5s"
    timeout  = "5s"
    retries  = 10
  }
}
