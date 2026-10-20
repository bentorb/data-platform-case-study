"""Stand-ins for the dbt_utils macros the models use, for sqlfluff's Jinja templater
(dbt/.sqlfluff): it renders models without dbt, so package macros are not available.
Only the SQL's shape matters for linting."""


def generate_surrogate_key(field_list):
    return "md5(" + " || '-' || ".join(f"coalesce(cast({f} as text), '')" for f in field_list) + ")"
