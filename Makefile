# Binance trades -> Pub/Sub -> Dataflow -> BigQuery
# Run `make help` for the list of targets.

SHELL := /bin/bash
.DEFAULT_GOAL := help

-include config.env
export

TF_DIR := infra/terraform
TF_VARS := TF_VAR_project_id=$(PROJECT_ID) TF_VAR_region=$(REGION) TF_VAR_zone=$(ZONE) \
           TF_VAR_billing_account=$(BILLING_ACCOUNT) TF_VAR_budget_amount=$(BUDGET_AMOUNT) \
           TF_VAR_symbols=$(SYMBOLS) TF_VAR_streams=$(STREAMS)
VENV := .venv

.PHONY: help bootstrap infra plan build up down status logs ssh test producer-local destroy check-config

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

check-config:
	@test -f config.env || (echo "config.env missing: cp config.env.example config.env" && exit 1)

# ---------------------------------------------------------------- one-time setup
bootstrap: check-config ## Create GCP project, link billing, create Terraform state bucket
	@scripts/bootstrap.sh

infra: check-config ## terraform init + apply (Pub/Sub, BigQuery, VM, IAM, budget, ...)
	terraform -chdir=$(TF_DIR) init -input=false -backend-config="bucket=$(PROJECT_ID)-tfstate"
	$(TF_VARS) terraform -chdir=$(TF_DIR) apply -input=false

plan: check-config ## terraform plan
	$(TF_VARS) terraform -chdir=$(TF_DIR) plan -input=false

build: check-config ## Build the pipeline image (Cloud Build) + Flex Template spec
	@scripts/build.sh

# ---------------------------------------------------------------- sessions
up: check-config ## START billing: launch Dataflow job + start producer VM
	@scripts/up.sh

down: check-config ## STOP billing: stop producer VM + drain Dataflow job
	@scripts/down.sh

status: check-config ## What is running + rows landed in the last 15 min
	@scripts/status.sh

logs: check-config ## Producer logs from Cloud Logging (last 30 min)
	@scripts/logs.sh

ssh: check-config ## SSH into the producer VM through IAP
	gcloud compute ssh $$(terraform -chdir=$(TF_DIR) output -raw producer_vm) \
	  --project $(PROJECT_ID) --zone $(ZONE) --tunnel-through-iap

# ---------------------------------------------------------------- local dev
$(VENV)/bin/activate: requirements-dev.txt producer/requirements.txt
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q --upgrade pip
	$(VENV)/bin/pip install -q -r requirements-dev.txt
	touch $@

test: $(VENV)/bin/activate ## Run unit tests
	$(VENV)/bin/pytest -q

producer-local: $(VENV)/bin/activate ## Stream live Binance trades to stdout (no GCP needed)
	$(VENV)/bin/python producer/producer.py --stdout --max-messages 20

# ---------------------------------------------------------------- teardown
destroy: check-config ## Delete ALL project resources (keeps the project + state bucket)
	-@scripts/down.sh
	$(TF_VARS) terraform -chdir=$(TF_DIR) destroy -input=false
