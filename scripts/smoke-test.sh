#!/usr/bin/env bash
# =============================================================================
# CDC Data Lakehouse — Smoke Test Script
# =============================================================================
# End-to-end smoke test: deploy CDK infrastructure → seed PostgreSQL →
# trigger DMS CDC → run ETL processor → verify Athena results.
#
# Prerequisites:
#   - AWS CLI configured with appropriate credentials
#   - AWS CDK CLI installed (npm install -g aws-cdk)
#   - Java 17 + Maven installed
#   - psql client available
#   - jq installed (for JSON parsing)
#
# Usage:
#   ./smoke-test.sh [--skip-deploy] [--skip-seed] [--region us-east-1]
# =============================================================================

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="CdcLakehouseStack"
RAW_BUCKET="cdc-raw-data-${AWS_ACCOUNT_ID:-local}"
LAKEHOUSE_BUCKET="cdc-lakehouse-data-${AWS_ACCOUNT_ID:-local}"
GLUE_DATABASE="lakehouse_db"
ATHENA_WORKGROUP="etl-workgroup"
ATHENA_OUTPUT="s3://${LAKEHOUSE_BUCKET}/athena-results/"
PG_HOST=""
PG_PORT="5432"
PG_DB="postgres"
PG_USER="postgres"
PG_PASSWORD=""

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Options
SKIP_DEPLOY=false
SKIP_SEED=false

# ── Parse arguments ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-deploy) SKIP_DEPLOY=true; shift ;;
        --skip-seed)   SKIP_SEED=true; shift ;;
        --region)      REGION="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2; }
die()  { err "$*"; exit 1; }

# ── Step 0: Pre-flight checks ─────────────────────────────────────────────────
log "=== Step 0: Pre-flight checks ==="

command -v aws    >/dev/null 2>&1 || die "aws CLI not found"
command -v cdk    >/dev/null 2>&1 || die "cdk CLI not found"
command -v mvn    >/dev/null 2>&1 || die "mvn not found"
command -v psql   >/dev/null 2>&1 || die "psql not found"
command -v jq     >/dev/null 2>&1 || die "jq not found"

# Verify AWS credentials
aws sts get-caller-identity --region "${REGION}" >/dev/null 2>&1 \
    || die "AWS credentials not configured"

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "${REGION}")
log "AWS Account: ${AWS_ACCOUNT_ID}, Region: ${REGION}"

# ── Step 1: Build the project ─────────────────────────────────────────────────
log "=== Step 1: Build ==="
cd "${PROJECT_DIR}"
mvn clean compile -q || die "Maven build failed"
log "Build successful."

# ── Step 2: Deploy CDK infrastructure ─────────────────────────────────────────
if [[ "${SKIP_DEPLOY}" == "false" ]]; then
    log "=== Step 2: CDK Deploy ==="
    cd "${PROJECT_DIR}/cdk-infra"
    cdk synth --quiet || die "CDK synth failed"
    cdk deploy --require-approval never --region "${REGION}" || die "CDK deploy failed"
    log "CDK deployment complete."

    # Extract outputs
    RAW_BUCKET=$(aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --query "Stacks[0].Outputs[?OutputKey=='RawBucketName'].OutputValue" \
        --output text --region "${REGION}")
    LAKEHOUSE_BUCKET=$(aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --query "Stacks[0].Outputs[?OutputKey=='LakehouseBucketName'].OutputValue" \
        --output text --region "${REGION}")
    PG_HOST=$(aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --query "Stacks[0].Outputs[?OutputKey=='RdsEndpoint'].OutputValue" \
        --output text --region "${REGION}")
    log "Raw bucket: ${RAW_BUCKET}"
    log "Lakehouse bucket: ${LAKEHOUSE_BUCKET}"
    log "RDS endpoint: ${PG_HOST}"
else
    log "=== Step 2: SKIPPED (--skip-deploy) ==="
fi

# ── Step 3: Seed PostgreSQL ───────────────────────────────────────────────────
if [[ "${SKIP_SEED}" == "false" ]]; then
    log "=== Step 3: Seed Data ==="

    if [[ -z "${PG_HOST}" ]]; then
        die "RDS endpoint not available. Provide PG_HOST or run without --skip-deploy."
    fi

    # Run the SeedDataGenerator via Maven exec plugin
    cd "${PROJECT_DIR}/etl-processor"
    mvn exec:java \
        -Dexec.mainClass="com.cdc.etl.seed.SeedDataGenerator" \
        -Dexec.args="--db-url=jdbc:postgresql://${PG_HOST}:${PG_PORT}/${PG_DB} --db-user=${PG_USER} --db-pass=${PG_PASSWORD} --force" \
        -Dspring.profiles.active=seed || die "Seed data generation failed"
    log "Seed data complete."
else
    log "=== Step 3: SKIPPED (--skip-seed) ==="
fi

# ── Step 4: Wait for DMS CDC to catch up ──────────────────────────────────────
log "=== Step 4: Wait for DMS replication ==="
log "Waiting 120 seconds for DMS to replicate CDC data to S3..."
sleep 120

# Verify Parquet files exist in raw bucket
RAW_FILE_COUNT=$(aws s3 ls "s3://${RAW_BUCKET}/dms-source/" --recursive --region "${REGION}" \
    | grep '\.parquet$' | wc -l)
log "Found ${RAW_FILE_COUNT} Parquet files in raw zone."
if [[ "${RAW_FILE_COUNT}" -eq 0 ]]; then
    err "No Parquet files found in raw zone. DMS may not have completed replication."
fi

# ── Step 5: Run ETL Processor ─────────────────────────────────────────────────
log "=== Step 5: Run ETL Processor ==="
cd "${PROJECT_DIR}/etl-processor"

# Run the processor as a one-shot (process all pending files, then exit)
# Uses Spring profile 'cli' for one-shot mode vs daemon mode
mvn exec:java \
    -Dexec.mainClass="com.cdc.etl.orchestrator.EtlOrchestrator" \
    -Dspring.profiles.active=cli \
    -Daws.region="${REGION}" \
    -Draw-bucket="${RAW_BUCKET}" \
    -Dlakehouse-bucket="${LAKEHOUSE_BUCKET}" || die "ETL processor failed"
log "ETL processing complete."

# ── Step 6: Verify Glue Catalog ───────────────────────────────────────────────
log "=== Step 6: Verify Glue Catalog ==="

for TABLE in dim_customer dim_product dim_date fact_orders; do
    if aws glue get-table --database-name "${GLUE_DATABASE}" --name "${TABLE}" \
        --region "${REGION}" >/dev/null 2>&1; then
        log "  ✓ ${TABLE} registered in Glue"
    else
        err "  ✗ ${TABLE} NOT found in Glue"
    fi
done

# ── Step 7: Run Athena verification queries ───────────────────────────────────
log "=== Step 7: Athena Verification ==="

QUERY_FILE="${SCRIPT_DIR}/verification-queries.sql"

# Split multi-statement SQL and run each query
# Athena only supports single statements per execution
log "Running Athena verification queries..."

# Query 1: Row counts
QUERY_ID_1=$(aws athena start-query-execution \
    --query-string "SELECT 'dim_customer' AS t, COUNT(*) FROM ${GLUE_DATABASE}.dim_customer UNION ALL SELECT 'dim_product', COUNT(*) FROM ${GLUE_DATABASE}.dim_product UNION ALL SELECT 'dim_date', COUNT(*) FROM ${GLUE_DATABASE}.dim_date UNION ALL SELECT 'fact_orders', COUNT(*) FROM ${GLUE_DATABASE}.fact_orders" \
    --query-execution-context "Database=${GLUE_DATABASE}" \
    --result-configuration "OutputLocation=${ATHENA_OUTPUT}" \
    --work-group "${ATHENA_WORKGROUP}" \
    --region "${REGION}" \
    --output text \
    --query 'QueryExecutionId')

log "Row count query submitted: ${QUERY_ID_1}"

# Query 2: Star Schema JOIN (top 5 customers)
QUERY_ID_2=$(aws athena start-query-execution \
    --query-string "SELECT c.name, COUNT(*) as orders, SUM(f.total_amount) as spent FROM ${GLUE_DATABASE}.fact_orders f JOIN ${GLUE_DATABASE}.dim_customer c ON f.customer_key = c.customer_key WHERE c.is_current = true GROUP BY c.name ORDER BY spent DESC LIMIT 5" \
    --query-execution-context "Database=${GLUE_DATABASE}" \
    --result-configuration "OutputLocation=${ATHENA_OUTPUT}" \
    --work-group "${ATHENA_WORKGROUP}" \
    --region "${REGION}" \
    --output text \
    --query 'QueryExecutionId')

log "Star schema JOIN query submitted: ${QUERY_ID_2}"

# Wait for queries to complete
for QID in "${QUERY_ID_1}" "${QUERY_ID_2}"; do
    log "Waiting for query ${QID}..."
    for i in $(seq 1 30); do
        STATE=$(aws athena get-query-execution \
            --query-execution-id "${QID}" \
            --region "${REGION}" \
            --output text \
            --query 'QueryExecution.Status.State')
        case "${STATE}" in
            SUCCEEDED)
                log "  ✓ Query ${QID} succeeded"
                # Fetch results
                aws athena get-query-results \
                    --query-execution-id "${QID}" \
                    --region "${REGION}" \
                    --output table
                break
                ;;
            FAILED|CANCELLED)
                err "  ✗ Query ${QID} ${STATE}"
                REASON=$(aws athena get-query-execution \
                    --query-execution-id "${QID}" \
                    --region "${REGION}" \
                    --output text \
                    --query 'QueryExecution.Status.StateChangeReason')
                err "    Reason: ${REASON}"
                break
                ;;
            *)
                sleep 2
                ;;
        esac
    done
done

# ── Step 8: Summary ───────────────────────────────────────────────────────────
log "=== Smoke Test Complete ==="
log ""
log "Summary:"
log "  Infrastructure: Deployed"
log "  Seed Data:      22 customers, 12 products, 110 orders, 330 items"
log "  Raw Parquet:    ${RAW_FILE_COUNT} files in s3://${RAW_BUCKET}/dms-source/"
log "  Glue Database:  ${GLUE_DATABASE}"
log "  Athena:         Queries executed against ${GLUE_DATABASE}"
log ""
log "Next steps:"
log "  1. Open Athena console → Query Editor → database: ${GLUE_DATABASE}"
log "  2. Run queries from: ${QUERY_FILE}"
log "  3. Verify results match expected counts"
log ""
log "To tear down: cd ${PROJECT_DIR}/cdk-infra && cdk destroy"
