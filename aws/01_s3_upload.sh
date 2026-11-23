#!/usr/bin/env bash
# BankLake - AWS Stage 1, step 2: create the S3 bucket and upload the lake.
#
# Run from the project root on your Mac:   bash aws/01_s3_upload.sh
# Safe to re-run: it only uploads what changed.
#
# Uploads: data/lake/silver and gold (Silver is masked; Gold customer_360 is EXCLUDED because Gold holds unmasked PII).
# Never uploads: pii_vault.db, banklake_meta.db, quarantine/, .env, raw/ (unless UPLOAD_RAW=1).

set -euo pipefail

REGION="ap-southeast-2"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAKE="$ROOT/data/lake"
RAW="$ROOT/data/raw"

command -v aws >/dev/null || { echo "aws CLI not found"; exit 1; }

ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="${BANKLAKE_BUCKET:-banklake-${ACCOUNT}-aps2}"

echo "Region : $REGION"
echo "Bucket : s3://$BUCKET"
echo

for layer in silver gold; do
  if [ ! -d "$LAKE/$layer" ]; then
    echo "Missing $LAKE/$layer"
    echo "Build the lake first:  python main.py demo --scale small"
    exit 1
  fi
done

# 1. Create the bucket if it does not exist yet.
if aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
  echo "Bucket already exists, reusing it."
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION" >/dev/null
  echo "Bucket created."
fi

# 2. Keep it private.
aws s3api put-public-access-block --bucket "$BUCKET" --region "$REGION" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

# 3. Upload silver and gold. Gold customer_360 holds UNMASKED PII (Gold is the unmasked layer), so it is excluded.
#    Bronze and silver_merge stay local; quarantine, the PII vault and the metadata DB are never uploaded.
for layer in silver gold; do
  echo "Uploading $layer ..."
  aws s3 sync "$LAKE/$layer" "s3://$BUCKET/lake/$layer" \
    --region "$REGION" --exclude "*.crc" --exclude ".DS_Store" --exclude "customer_360/*" --delete --only-show-errors
done

# 4. Optional: raw source files (synthetic, but they contain unmasked demo PII).
if [ "${UPLOAD_RAW:-0}" = "1" ]; then
  echo "Uploading raw ..."
  aws s3 sync "$RAW" "s3://$BUCKET/raw" --region "$REGION" \
    --exclude ".DS_Store" --only-show-errors
fi

echo
echo "Done. Contents of the bucket:"
aws s3 ls "s3://$BUCKET/lake/" --region "$REGION"
echo
aws s3 ls "s3://$BUCKET/" --recursive --summarize --human-readable --region "$REGION" | tail -2
