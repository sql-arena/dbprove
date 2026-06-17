# AWS ECR GitHub Actions

This repository publishes Docker images to Amazon ECR for ECS or other runtime use.

## AWS resources

- AWS account: `578125593767`
- Region: `eu-north-1`
- ECR repository: `578125593767.dkr.ecr.eu-north-1.amazonaws.com/dbprove`
- GitHub Actions IAM role: `arn:aws:iam::578125593767:role/dbprove-github-actions-ecr`
- GitHub OIDC provider: `arn:aws:iam::578125593767:oidc-provider/token.actions.githubusercontent.com`

## Workflow

The workflow lives in [.github/workflows/publish-docker-images.yml](/Users/thomaskejser/source/dbprove/.github/workflows/publish-docker-images.yml:1).

It runs on:

- pushes to `main` that touch `docker/**`
- manual `workflow_dispatch`

It watches these build contexts:

- `docker/clickhouse/`
- `docker/datafusion/`
- `docker/mssql/`
- `docker/postgresql/`
- `docker/trino/`

Only changed services are rebuilt on normal pushes. Manual runs rebuild all services.

## Tags

All images are pushed into the single `dbprove` repository with service-prefixed tags:

- `<service>-latest`
- `<service>-sha-<git sha>`

Examples:

- `dbprove:trino-latest`
- `dbprove:trino-sha-<git sha>`
- `dbprove:datafusion-latest`

## Authentication model

GitHub Actions uses OpenID Connect to assume the AWS role directly. No long-lived AWS secrets are needed in GitHub for ECR publishing.
