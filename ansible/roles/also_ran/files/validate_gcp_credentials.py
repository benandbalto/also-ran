#!/usr/bin/env python3
"""Offline shape checks for administrator-owned explicit gcloud credentials."""

import json
import sys
from urllib.parse import urlsplit


def https_url(value):
    if not isinstance(value, str):
        return False
    parsed = urlsplit(value)
    return parsed.scheme == "https" and bool(parsed.hostname) and not parsed.username


def validate(mode, config):
    if not isinstance(config, dict):
        raise ValueError("GCP credential must be a JSON object")
    if mode == "service_account_key":
        if config.get("type") != "service_account" or not all(
            isinstance(config.get(field), str) and config[field].strip()
            for field in ("client_email", "private_key", "token_uri")
        ) or not https_url(config["token_uri"]):
            raise ValueError("service_account_key requires a service_account JSON key")
        return
    if mode != "workload_identity_federation":
        raise ValueError("unsupported explicit GCP authentication mode")
    if config.get("type") != "external_account":
        raise ValueError("workload_identity_federation requires external_account JSON")
    if not all(isinstance(config.get(field), str) and config[field].strip()
               for field in ("audience", "subject_token_type")) or not https_url(config.get("token_url")):
        raise ValueError("external_account configuration is missing federation fields")
    source = config.get("credential_source")
    if not isinstance(source, dict) or "executable" in source:
        raise ValueError("use a file- or URL-sourced federation configuration")
    sources = set(source) & {"file", "url", "environment_id"}
    if sources == {"file"}:
        if not isinstance(source["file"], str) or not source["file"].startswith("/"):
            raise ValueError("federation subject-token file must use an absolute host path")
    elif sources == {"url"}:
        if not https_url(source["url"]):
            raise ValueError("federation subject-token URL must use HTTPS")
    else:
        raise ValueError("use exactly one supported file or URL federation source")
    if "service_account_impersonation_url" in config and not https_url(config["service_account_impersonation_url"]):
        raise ValueError("service account impersonation URL must use HTTPS")


if __name__ == "__main__":
    try:
        validate(sys.argv[1], json.load(sys.stdin))
    except (ValueError, KeyError, IndexError, TypeError):
        # Neither credential contents nor provider URLs belong in Ansible logs.
        sys.exit("Invalid GCP credential configuration for the selected authentication mode")
