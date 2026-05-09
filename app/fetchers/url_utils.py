import urllib.parse


TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "dclid",
    "gbraid",
    "wbraid",
    "guccounter",
    "mc_cid",
    "mc_eid",
    "igshid",
    "ref",
    "ref_src",
    "smid",
    "cmpid",
}


def normalize_canonical_url(url):
    """
    Return a stable article URL for duplicate checks.

    The normalizer intentionally stays conservative: it lowercases the scheme and
    host, strips fragments and common tracking parameters, and unwraps obvious
    redirect parameters when a URL carries the real article URL in a query value.
    """
    if not url:
        return ""

    url = url.strip()
    parsed = urllib.parse.urlsplit(url)

    redirect_url = _extract_redirect_url(parsed)
    if redirect_url and redirect_url != url:
        return normalize_canonical_url(redirect_url)

    query_pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    cleaned_pairs = []
    for key, value in query_pairs:
        key_lower = key.lower()
        if key_lower.startswith("utm_") or key_lower in TRACKING_PARAMS:
            continue
        cleaned_pairs.append((key, value))

    cleaned_query = urllib.parse.urlencode(cleaned_pairs, doseq=True)
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return urllib.parse.urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            path,
            cleaned_query,
            "",
        )
    )


def _extract_redirect_url(parsed):
    if not parsed.query:
        return None

    redirect_param_names = {"url", "u", "target", "redirect", "redirect_url"}
    for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() not in redirect_param_names:
            continue

        unquoted = urllib.parse.unquote(value)
        nested = urllib.parse.urlsplit(unquoted)
        if nested.scheme in {"http", "https"} and nested.netloc:
            return unquoted

    return None
