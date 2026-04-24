from app.api.client import PCMClient


def list_designs(client: PCMClient = None):
    """List all available designs."""
    client = client or PCMClient.from_config()
    result = client.get("/design")
    return result.get("results", [])


def get_design(design_id, client: PCMClient = None):
    """Get a specific design by ID."""
    client = client or PCMClient.from_config()
    return client.get(f"/design/{design_id}")


def get_design_sizes():
    """Available postcard sizes."""
    return [
        {"key": "46", "label": "4x6"},
        {"key": "46S", "label": "4x6 Single"},
        {"key": "58", "label": "5x8"},
        {"key": "68", "label": "6x8"},
        {"key": "69", "label": "6x9"},
        {"key": "611", "label": "6x11"},
        {"key": "651", "label": "6.5x1"},
    ]
