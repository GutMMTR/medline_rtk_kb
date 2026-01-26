from __future__ import annotations

import datetime as dt
import posixpath
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote, unquote, urlparse

import httpx


DAV_NS = {"d": "DAV:"}


@dataclass(frozen=True)
class DavItem:
    name: str
    href: str
    is_dir: bool
    etag: str | None
    size_bytes: int | None
    last_modified: dt.datetime | None


def _safe_join(*parts: str) -> str:
    clean = [p.strip("/").strip() for p in parts if p is not None and str(p).strip("/").strip()]
    return "/".join(clean)


def _rfc1123_to_dt(value: str) -> dt.datetime | None:
    # Example: 'Wed, 21 Oct 2015 07:28:00 GMT'
    try:
        return dt.datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %Z")
    except Exception:
        return None


class NextcloudDavClient:
    """
    WebDAV client for Nextcloud.
    We support two common endpoints:
      - https://host/remote.php/webdav/                  (preferred; user is inferred from auth)
      - https://host/remote.php/dav/files/<username>/    (fallback)
    """

    def __init__(self, base_webdav_url: str, username: str, password: str, timeout_s: int = 60):
        # base_webdav_url can be either:
        #  - site url (https://host)
        #  - full webdav url (https://host/remote.php/webdav/)
        site = _normalize_site_base(base_webdav_url)
        self.base_primary = _ensure_trailing_slash(_safe_join(site, "remote.php", "webdav"))
        uid = _guess_nextcloud_uid(username)
        self.base_fallback = _ensure_trailing_slash(_safe_join(site, "remote.php", "dav", "files", uid)) if uid else None

        self._client = httpx.Client(
            timeout=timeout_s,
            auth=(username, password),
            follow_redirects=True,
        )

    def _url(self, base: str, rel_path: str) -> str:
        rel_path = (rel_path or "").strip("/")
        if not rel_path:
            return base
        # encode each segment (incl. cyrillic)
        enc = "/".join(quote(p) for p in rel_path.split("/"))
        return _ensure_trailing_slash(base) + enc

    def _request(self, method: str, rel_path: str, *, headers: dict[str, str] | None = None, content: bytes | str | None = None) -> httpx.Response:
        # Try primary endpoint first; if 404, fallback to dav/files/<uid>.
        bases = [self.base_primary]
        if self.base_fallback:
            bases.append(self.base_fallback)

        last_err: Exception | None = None
        for idx, base in enumerate(bases):
            try:
                url = self._url(base, rel_path)
                resp = self._client.request(method, url, headers=headers, content=content)
                # If primary endpoint is not supported on this server, try fallback.
                if resp.status_code == 404 and idx == 0 and len(bases) > 1:
                    continue
                resp.raise_for_status()
                return resp
            except Exception as e:
                last_err = e
                # If 404 on primary - try fallback.
                if isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 404 and idx == 0 and len(bases) > 1:
                    continue
                raise
        assert last_err is not None
        raise last_err

    def propfind(self, rel_path: str, depth: int = 1) -> list[DavItem]:
        body = (
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
            "<d:propfind xmlns:d=\"DAV:\">"
            "<d:prop>"
            "<d:resourcetype/>"
            "<d:getetag/>"
            "<d:getcontentlength/>"
            "<d:getlastmodified/>"
            "</d:prop>"
            "</d:propfind>"
        )
        resp = self._request("PROPFIND", rel_path, headers={"Depth": str(depth)}, content=body)

        root = ET.fromstring(resp.content)
        items: list[DavItem] = []
        for r in root.findall("d:response", DAV_NS):
            href = r.findtext("d:href", default="", namespaces=DAV_NS) or ""
            prop = r.find(".//d:prop", DAV_NS)
            if prop is None:
                continue
            is_dir = prop.find("d:resourcetype/d:collection", DAV_NS) is not None
            etag = prop.findtext("d:getetag", default=None, namespaces=DAV_NS)
            size_txt = prop.findtext("d:getcontentlength", default=None, namespaces=DAV_NS)
            lm_txt = prop.findtext("d:getlastmodified", default=None, namespaces=DAV_NS)
            size = None
            if size_txt and str(size_txt).strip().isdigit():
                size = int(str(size_txt).strip())
            lm = _rfc1123_to_dt(lm_txt) if lm_txt else None
            # href in WebDAV responses is typically percent-encoded; decode for UI and internal path joining
            name = unquote(href.rstrip("/").split("/")[-1] or "")
            items.append(DavItem(name=name, href=href, is_dir=is_dir, etag=etag, size_bytes=size, last_modified=lm))

        # Nextcloud usually includes the "self" folder as the first entry in PROPFIND depth=1.
        # Filter it out to avoid showing root folder as an organization.
        rel_path_norm = (rel_path or "").strip("/")
        if not rel_path_norm:
            # for true root, self entry typically has empty name; keep only items that have a name
            return [it for it in items if it.name]

        last_seg = rel_path_norm.split("/")[-1]
        filtered: list[DavItem] = []
        for it in items:
            href_raw = (it.href or "").strip()
            # href may be absolute URL or path; normalize to decoded path for robust matching
            href_path = urlparse(href_raw).path if "://" in href_raw else href_raw
            href_path_dec = unquote((href_path or "").rstrip("/"))
            is_self = it.is_dir and it.name == last_seg and (
                href_path_dec == rel_path_norm or href_path_dec.endswith("/" + rel_path_norm)
            )
            if is_self:
                continue
            filtered.append(it)
        return filtered

    def download(self, rel_path: str) -> tuple[bytes, str]:
        resp = self._request("GET", rel_path)
        ctype = resp.headers.get("Content-Type") or "application/octet-stream"
        return resp.content, ctype


def build_webdav_base_url(base_url: str, username: str) -> str:
    # Backward compatible: user passes a site URL in UI (https://host),
    # and client chooses the best DAV endpoint automatically.
    # We return site base (without /remote.php/...), not a full endpoint.
    _ = username  # kept for compatibility
    return _normalize_site_base(base_url)


def _ensure_trailing_slash(url: str) -> str:
    return url.rstrip("/") + "/"


def _normalize_site_base(base_url: str) -> str:
    # Accept:
    #  - https://host
    #  - https://host/
    #  - https://host/remote.php/webdav
    #  - https://host/remote.php/dav/files/<uid>/
    s = (base_url or "").strip()
    if not s:
        return ""
    s = s.rstrip("/")
    cut = s.find("/remote.php/")
    if cut != -1:
        s = s[:cut]
    return s.rstrip("/")


def _guess_nextcloud_uid(username: str) -> str:
    # Many deployments use "uid" (e.g. agutman) in /dav/files/<uid>/,
    # but login form may accept email. Best effort:
    u = (username or "").strip()
    if not u:
        return ""
    if "@" in u:
        return u.split("@", 1)[0]
    return u

