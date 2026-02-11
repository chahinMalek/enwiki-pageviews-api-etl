from urllib.parse import unquote


def decode_title(title: str) -> str:
    """URL-decode a Wikipedia article title and replace underscores with spaces."""
    return unquote(title).replace("_", " ")
