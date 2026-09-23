import os


def clean_setting(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().strip("`").strip().strip('"').strip("'").strip()
    return text or None


def get_first_setting(*names: str, default: str | None = None) -> str | None:
    for name in names:
        if not name:
            continue
        value = clean_setting(os.getenv(name))
        if value is not None:
            return value
    return default


def get_required_setting(*names: str) -> str:
    value = get_first_setting(*names)
    if value is None:
        raise ValueError(f"Missing required setting: {' or '.join(name for name in names if name)}")
    return value


def get_bool_setting(*names: str, default: bool = False) -> bool:
    value = get_first_setting(*names)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "y", "on"}


def get_int_setting(*names: str, default: int, minimum: int | None = None) -> int:
    value = get_first_setting(*names)
    try:
        parsed = int(str(value).strip()) if value is not None else default
    except Exception:
        parsed = default
    if minimum is not None and parsed < minimum:
        return minimum
    return parsed


def get_float_setting(*names: str, default: float, minimum: float | None = None) -> float:
    value = get_first_setting(*names)
    try:
        parsed = float(str(value).strip()) if value is not None else default
    except Exception:
        parsed = default
    if minimum is not None and parsed < minimum:
        return minimum
    return parsed


def build_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def get_integration_active_env() -> str:
    active_env = (get_first_setting("INTEGRATION_ACTIVE_ENV", default="uat") or "uat").lower()
    return "prod" if active_env == "prod" else "uat"


def get_integration_env_setting(name_suffix: str, *legacy_names: str, default: str | None = None) -> str | None:
    active_env = get_integration_active_env().upper()
    return get_first_setting(
        f"INTEGRATION_{active_env}_{name_suffix}",
        f"INTEGRATION_{name_suffix}",
        *legacy_names,
        default=default,
    )


def get_integration_base_url(*legacy_base_url_names: str) -> str | None:
    direct_base_url = get_first_setting("INTEGRATION_BASE_URL")
    if direct_base_url:
        return direct_base_url
    return get_integration_env_setting("BASE_URL", *legacy_base_url_names)
