from app.api.client import PCMClient


def create_postcard_order(recipients, client=None, design_id=None,
                          front_url=None, back_url=None, size="46",
                          mail_class="FirstClass", scheduled_date=None):
    """
    Create a postcard order.

    Args:
        recipients: list of dicts with keys: firstName, lastName, address, city, state, zip
                    and optional: address2, company, designFields (for merge/personalization)
        design_id: PCM design ID (use this OR front_url/back_url)
        front_url: URL to front design PDF/image
        back_url: URL to back design PDF/image
        size: postcard size (46, 58, 68, 69, 611, 651)
        mail_class: FirstClass or Standard
        scheduled_date: optional date string (YYYY-MM-DD) to schedule mailing
    """
    client = client or PCMClient.from_config()

    payload = {
        "mailClass": mail_class,
        "recipients": recipients,
    }

    if design_id:
        payload["designID"] = design_id
    else:
        payload["size"] = size
        if front_url:
            payload["front"] = front_url
        if back_url:
            payload["back"] = back_url

    if scheduled_date:
        payload["mailingDate"] = scheduled_date

    return client.post("/order/postcard", data=payload)


def create_letter_order(recipients, client=None, design_id=None,
                        letter_url=None, mail_class="FirstClass",
                        color=True, print_both_sides=False,
                        insert_addressing_page=True,
                        envelope=None, scheduled_date=None):
    """
    Create a letter order.

    Args:
        recipients: list of dicts with keys: firstName, lastName, address, city, state, zip
        design_id: PCM design ID (use this OR letter_url)
        letter_url: URL to letter PDF
        mail_class: FirstClass or Standard
        envelope: dict, e.g. {"type": "NbrTenWindow"} or {"type": "Flat", "size": "6x9"}
        scheduled_date: optional date string (YYYY-MM-DD)
    """
    client = client or PCMClient.from_config()

    payload = {
        "mailClass": mail_class,
        "color": color,
        "printOnBothSides": print_both_sides,
        "insertAddressingPage": insert_addressing_page,
        "envelope": envelope or {"type": "NbrTenWindow"},
        "recipients": recipients,
    }

    if design_id:
        payload["designID"] = design_id
    elif letter_url:
        payload["letter"] = letter_url

    if scheduled_date:
        payload["mailingDate"] = scheduled_date

    return client.post("/order/letter", data=payload)


def get_orders(client=None, page=1, per_page=10):
    """List orders with pagination."""
    client = client or PCMClient.from_config()
    return client.get(f"/order?page={page}&perPage={per_page}")


def get_order_status(order_id, client=None):
    """Get status of a specific order."""
    client = client or PCMClient.from_config()
    return client.get(f"/order/{order_id}")


def cancel_order(order_id, client=None):
    """Cancel an order (must be before 11 PM EST cutoff)."""
    client = client or PCMClient.from_config()
    return client.delete(f"/order/{order_id}")


def build_recipient(lead):
    """Convert a Lead model instance to a PCM recipient dict."""
    parts = lead.street.split()
    recipient = {
        "address": lead.street,
        "city": lead.city,
        "state": lead.state,
        "zip": lead.zip_code,
    }
    # Add name fields if we have them (probate leads)
    if lead.decedent_name:
        name_parts = lead.decedent_name.split(None, 1)
        recipient["firstName"] = name_parts[0] if name_parts else "Current"
        recipient["lastName"] = name_parts[1] if len(name_parts) > 1 else "Resident"
    else:
        recipient["firstName"] = "Current"
        recipient["lastName"] = "Resident"

    return recipient
