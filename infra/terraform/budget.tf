# Alerts only: budgets never stop spending. `make down` is the off switch.
resource "google_billing_budget" "monthly" {
  provider        = google.billing
  billing_account = var.billing_account
  display_name    = "${var.project_id} monthly budget"

  budget_filter {
    projects        = ["projects/${data.google_project.this.number}"]
    calendar_period = "MONTH"
  }

  amount {
    specified_amount {
      # No currency_code: uses the billing account's currency (USD or CHF).
      units = tostring(var.budget_amount)
    }
  }

  threshold_rules {
    threshold_percent = 0.5
  }
  threshold_rules {
    threshold_percent = 0.9
  }
  threshold_rules {
    threshold_percent = 1.0
  }
  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "FORECASTED_SPEND"
  }

  # Emails go to billing account admins/users by default.
  depends_on = [google_project_service.services]
}
