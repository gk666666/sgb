#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


KEEP_EXACT = {
    "FUNCTIONS_EXTENSION_VERSION",
    "FUNCTIONS_WORKER_RUNTIME",
    "AzureFunctionsJobHost__functionTimeout",
    "SCM_DO_BUILD_DURING_DEPLOYMENT",
    "EXPORT_PIPELINE_NAME",
}

KEEP_SUFFIXES = (
    ".Disabled",
    "_SCHEDULE",
    "_BLOB_NAME",
    "_CONTAINER_NAME",
    "_SUCCESS_TABLE",
    "_AUDIT_TABLE",
    "_FORWARD_TARGET",
)

KEEP_PREFIXES = (
    "TRACEABILITY_DEFECT_SUCCESS_",
    "GLASS_MASTER_SUCCESS_",
    "PRODUCTION_OUTPUT_SUCCESS_",
)

PLACEHOLDER_BY_NAME = {
    "ADX_CLUSTER_URL": "<adx-cluster-url>",
    "ADX_DATABASE": "<adx-database>",
    "ADX_TOKEN_SCOPE": "<adx-token-scope>",
    "APPINSIGHTS_INSTRUMENTATIONKEY": "<appinsights-instrumentation-key>",
    "APPLICATIONINSIGHTS_CONNECTION_STRING": "<appinsights-connection-string>",
    "AzureWebJobsStorage": "<azure-webjobs-storage-connection-string>",
    "STATE_STORAGE_ACCOUNT_URL": "<state-storage-account-url>",
    "HTTP_PROXY": "<http-proxy-or-empty>",
    "HTTPS_PROXY": "<https-proxy-or-empty>",
    "NO_PROXY": "<no-proxy-or-empty>",
    "INTEGRATION_ACTIVE_ENV": "<uat-or-prod>",
    "INTEGRATION_TENANT_ID": "<tenant-id>",
    "SILK_SCREEN_EVENTHUB_CONNECTION": "<eventhub-connection-string>",
    "SILK_SCREEN_EVENTHUB_NAME": "<eventhub-name>",
    "SNOWFLAKE_ACCOUNT": "<snowflake-account>",
    "SNOWFLAKE_DATABASE": "<snowflake-database>",
    "SNOWFLAKE_SCHEMA": "<snowflake-schema>",
    "SNOWFLAKE_TABLE": "<snowflake-table>",
    "SNOWFLAKE_USER": "<snowflake-user>",
    "SNOWFLAKE_ROLE": "<snowflake-role>",
    "SNOWFLAKE_WAREHOUSE": "<snowflake-warehouse>",
    "SNOWFLAKE_EXPORT_CONTAINER": "<snowflake-export-container>",
    "SNOWFLAKE_EXPORT_PREFIX": "<snowflake-export-prefix>",
    "SNOWFLAKE_PRIVATE_KEY_PEM": "-----BEGIN ENCRYPTED PRIVATE KEY-----\\n<customer-private-key-pem>\\n-----END ENCRYPTED PRIVATE KEY-----",
    "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "<snowflake-private-key-passphrase>",
}


def sanitize_value(name: str, value: str) -> str:
    if name in PLACEHOLDER_BY_NAME:
        return PLACEHOLDER_BY_NAME[name]

    if name in KEEP_EXACT or any(name.endswith(suffix) for suffix in KEEP_SUFFIXES):
        return value

    if any(name.startswith(prefix) for prefix in KEEP_PREFIXES):
        return value

    if value in {"true", "false"}:
        return value

    if isinstance(value, str) and value.replace(".", "", 1).isdigit():
        return value

    if " " in value and "*" in value:
        return value

    if name.endswith("_INITIAL_WATERMARK"):
        return "<optional-initial-watermark>"
    if name.endswith("_APP_ID"):
        return "<app-id>"
    if name.endswith("_APP_SECRET"):
        return "<app-secret>"
    if name.endswith("_BASE_URL"):
        return "<base-url>"
    if name.endswith("_API_TIMEOUT"):
        return value
    if name.endswith("_API_BATCH_SIZE"):
        return value
    if name.endswith("_API_MAX_GROUPS_PER_BATCH"):
        return value
    if name.endswith("_API_BATCH_SLEEP_SECONDS"):
        return value
    if name.endswith("_OVERLAP_MINUTES"):
        return value
    if name.endswith("_LOOKBACK_HOURS"):
        return value
    if name.endswith("_DRY_RUN"):
        return value
    if name.endswith("_WRITE_AUDIT"):
        return value
    if name.endswith("_PREFIX"):
        return value
    if name.endswith("_NAME"):
        return value

    if "CONNECTION" in name or "CONN" in name:
        return "<connection-string>"
    if "SECRET" in name:
        return "<secret>"
    if "PASSWORD" in name or "PASSPHRASE" in name:
        return "<secret>"
    if "URL" in name:
        return "<url>"
    if "TOKEN_SCOPE" in name:
        return "<token-scope>"

    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a git-safe app settings template from a real export.")
    parser.add_argument("--input", required=True, help="Path to the real app settings JSON file.")
    parser.add_argument("--output", required=True, help="Path to write the sanitized template JSON file.")
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    items = json.loads(input_path.read_text(encoding="utf-8"))
    sanitized = []
    for item in items:
        copied = dict(item)
        copied["value"] = sanitize_value(item.get("name", ""), item.get("value", ""))
        sanitized.append(copied)

    output_path.write_text(json.dumps(sanitized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
