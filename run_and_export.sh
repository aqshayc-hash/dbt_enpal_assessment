#!/bin/bash
# ============================================
# dbt Run & Export Script for macOS/Linux
# Runs dbt and exports mart models to CSV
# ============================================

set -e

echo "Running dbt..."
.venv/bin/dbt run

echo ""
echo "Exporting mart models to results folder..."

# Create results folder if it doesn't exist
mkdir -p results

# Export rep_sales_funnel_monthly
docker exec dbt_enpal_assessment-db-1 psql -U admin -d postgres -c "\COPY (SELECT * FROM public_pipedrive_analytics.rep_sales_funnel_monthly ORDER BY month, funnel_step) TO STDOUT WITH CSV HEADER" > results/rep_sales_funnel_monthly.csv
echo "  - rep_sales_funnel_monthly.csv"

# Export rep_sales_funnel_by_rep_monthly
docker exec dbt_enpal_assessment-db-1 psql -U admin -d postgres -c "\COPY (SELECT * FROM public_pipedrive_analytics.rep_sales_funnel_by_rep_monthly ORDER BY month, rep_name, funnel_step) TO STDOUT WITH CSV HEADER" > results/rep_sales_funnel_by_rep_monthly.csv
echo "  - rep_sales_funnel_by_rep_monthly.csv"

# Export fct_crm__activities
docker exec dbt_enpal_assessment-db-1 psql -U admin -d postgres -c "\COPY (SELECT * FROM public_pipedrive_analytics.fct_crm__activities ORDER BY activity_id) TO STDOUT WITH CSV HEADER" > results/fct_crm__activities.csv
echo "  - fct_crm__activities.csv"

# Export fct_crm__deal_history
docker exec dbt_enpal_assessment-db-1 psql -U admin -d postgres -c "\COPY (SELECT * FROM public_pipedrive_analytics.fct_crm__deal_history ORDER BY deal_id, valid_from) TO STDOUT WITH CSV HEADER" > results/fct_crm__deal_history.csv
echo "  - fct_crm__deal_history.csv"

# Export dim_users
docker exec dbt_enpal_assessment-db-1 psql -U admin -d postgres -c "\COPY (SELECT * FROM public_pipedrive_analytics.dim_users ORDER BY user_id) TO STDOUT WITH CSV HEADER" > results/dim_users.csv
echo "  - dim_users.csv"

echo ""
echo "Done! All mart models exported to results/ folder."
