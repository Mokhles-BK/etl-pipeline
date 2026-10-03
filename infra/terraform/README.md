# Infrastructure (Terraform)

Provisions a local Postgres container for the ETL pipeline with the Docker
provider: image, persistent volume, localhost-only port, and a healthcheck.
No cloud account is needed.

## Prerequisites

- Docker Desktop, running
- Terraform >= 1.5 (`winget install Hashicorp.Terraform` on Windows)

## Use

```powershell
cd infra/terraform
$env:TF_VAR_db_password = "choose-a-password"   # at least 8 characters
terraform init
terraform plan
terraform apply
```

Then point the pipeline at it:

```powershell
terraform output -raw env_file      # paste these lines into the project's .env
cd ../..
python -m etl.cli --with-warehouse  # the pipeline creates its own schemas
```

The container listens on `127.0.0.1:5433` by default, so it does not clash with
a Postgres already running on 5432. Change it with `-var host_port=...`.

Tear down (removes the container and its data volume):

```powershell
terraform destroy
```

## Notes

- `terraform.tfstate` contains the database password in plain text. It and
  `terraform.tfvars` are git-ignored: never commit them.
- Linux/macOS: set `docker_host = "unix:///var/run/docker.sock"`.
- The same layout extends to a managed cloud database (for example AWS RDS) by
  swapping the provider and the container resource for `aws_db_instance`; the
  variables and outputs stay the same.
