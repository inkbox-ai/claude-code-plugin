"""Compatibility names delegated to the current remote catalog."""

COMPATIBILITY_NAMES = {
    "inkbox_lookup_contact": "inkbox_contacts_list",
    "inkbox_list_contacts": "inkbox_contacts_list",
    "inkbox_get_contact": "inkbox_contact_get",
    "inkbox_create_contact": "inkbox_contact_create",
    "inkbox_update_contact": "inkbox_contact_update",
    "inkbox_delete_contact": "inkbox_contact_delete",
}


def compatibility_arguments(name, arguments, operation_id=None):
    if (
        name == "inkbox_lookup_contact"
        and len(
            [
                key
                for key in (
                    "email",
                    "phone",
                    "email_domain",
                    "email_contains",
                    "phone_contains",
                )
                if arguments.get(key)
            ]
        )
        != 1
    ):
        raise ValueError("Contact lookup requires exactly one lookup criterion")
    if name == "inkbox_list_contacts":
        return {"limit": 25, **arguments}
    if name == "inkbox_delete_contact":
        return {**arguments, "idempotency_key": operation_id}
    if name not in {"inkbox_create_contact", "inkbox_update_contact"}:
        return arguments
    fields = {key: value for key, value in arguments.items() if key != "contact_id"}
    for key, value_key in (("emails", "value"), ("phones", "value_e164")):
        if key in fields:
            fields[key] = [
                {value_key: value, "label": None, "is_primary": index == 0}
                for index, value in enumerate(fields[key])
            ]
    result = (
        {"contact": fields}
        if name == "inkbox_create_contact"
        else {"contact_id": arguments["contact_id"], "patch": fields}
    )
    return {**result, "idempotency_key": operation_id}
