from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.audit.service import write_audit_log
from app.core.config import settings
from app.db.models import (
    Artifact,
    FileVersion,
    NextcloudRemoteFileState,
    OrgArtifact,
    OrgArtifactStatus,
    Organization,
    User,
)
from app.integrations.nextcloud_dav import NextcloudDavClient


@dataclass
class SyncStats:
    orgs_seen: int = 0
    orgs_created: int = 0
    folders_seen: int = 0
    files_seen: int = 0
    files_downloaded: int = 0
    files_skipped: int = 0
    file_versions_created: int = 0
    errors: int = 0


def _norm_etag(etag: str | None) -> str:
    """
    Nextcloud may return ETag with quotes: "\"abcd...\"".
    Normalize to stable comparable string.
    """
    s = (etag or "").strip()
    if s.startswith("W/"):
        s = s[2:].strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1]
    return s.strip()


def _same_remote_file(*, state: NextcloudRemoteFileState, etag: str, size: int) -> bool:
    """
    Decide whether remote file is unchanged relative to stored state.
    - Prefer ETag when present (most reliable).
    - Size may be unknown (0) depending on server/permissions.
    """
    if not state.imported_file_version_id:
        return False
    st_etag = _norm_etag(state.etag)
    etag = _norm_etag(etag)
    if etag and st_etag and etag == st_etag:
        # If size is known, also require it to match. If size is unknown (0), rely on ETag.
        if size > 0 and state.size_bytes > 0:
            return state.size_bytes == size
        return True
    # Fallback: if no etag, compare size when both known.
    if not etag and size > 0 and state.size_bytes > 0:
        return state.size_bytes == size
    return False


@dataclass
class _TrieNode:
    children: dict[str, "_TrieNode"]
    terminal: bool = False


def _build_short_name_trie(short_names: list[str]) -> _TrieNode:
    root = _TrieNode(children={}, terminal=False)
    for sn in short_names:
        parts = [p.strip() for p in (sn or "").split(".") if p.strip()]
        if not parts:
            continue
        node = root
        for p in parts:
            node = node.children.setdefault(p, _TrieNode(children={}, terminal=False))
        node.terminal = True
    return root


def _join_path(*parts: str) -> str:
    clean = [p.strip("/").strip() for p in parts if p and str(p).strip("/").strip()]
    return "/".join(clean)


def _iter_existing_files_for_known_short_names(
    *,
    dav: NextcloudDavClient,
    org_root_folder: str,
    trie: _TrieNode,
    stats: SyncStats,
) -> list[tuple[str, str, object]]:
    """
    Traverse Nextcloud tree ONLY along known short_name prefixes and yield files
    for folders that match a complete short_name.

    Returns list of tuples: (short_name, folder_path, DavItem)
    """
    out: list[tuple[str, str, object]] = []
    stack: list[tuple[str, _TrieNode, list[str]]] = [(org_root_folder, trie, [])]

    while stack:
        folder, node, parts = stack.pop()
        stats.folders_seen += 1
        try:
            listing = dav.propfind(folder, depth=1)
        except Exception:
            # folder missing / access denied / etc -> skip
            continue

        if node.terminal:
            sn = ".".join(parts)
            for it in listing:
                if (not getattr(it, "is_dir", False)) and getattr(it, "name", ""):
                    out.append((sn, folder, it))

        for it in listing:
            if not getattr(it, "is_dir", False):
                continue
            name = getattr(it, "name", "") or ""
            if not name:
                continue
            child = node.children.get(name)
            if not child:
                continue
            child_folder = _join_path(folder, name)
            stack.append((child_folder, child, [*parts, name]))

    return out


def _sort_org_artifacts_by_point(oas: list[OrgArtifact], item_no_by_oa_id: dict[int, int | None]) -> list[OrgArtifact]:
    """
    Sort org artifacts by their 'пункт' (achievement_item_no).
    - Items with a number go first (1,2,3,...)
    - Items without number go last (single artifact case)
    """

    def key(oa: OrgArtifact) -> tuple[int, int, int]:
        item_no = item_no_by_oa_id.get(oa.id)
        return (1 if item_no is None else 0, int(item_no or 0), int(oa.id))

    return sorted(oas, key=key)


def _sort_files_for_mapping(files: list[object]) -> list[object]:
    # Stable and predictable ordering: by filename, then last_modified (if any)
    def key(f: object) -> tuple[str, float]:
        name = str(getattr(f, "name", "") or "")
        lm = getattr(f, "last_modified", None)
        ts = lm.timestamp() if lm else 0.0
        return (name.lower(), ts)

    return sorted(files, key=key)


def _ensure_org(db: Session, org_name: str, create_orgs: bool) -> Organization | None:
    org = db.query(Organization).filter(Organization.name == org_name).one_or_none()
    if org:
        return org
    if not create_orgs:
        return None
    org = Organization(name=org_name, created_via="nextcloud")
    db.add(org)
    db.flush()
    return org


def sync_from_nextcloud(
    *,
    db: Session,
    actor: User,
    dav: NextcloudDavClient,
    root_folder: str,
    create_orgs: bool,
    org_names_filter: list[str] | None = None,
    request=None,
) -> SyncStats:
    """
    Sync artifacts from Nextcloud WebDAV.

    Expected folder layout:
      <root>/<OrgName>/<short_name path>/<files>
    where short_name path is short_name split by '.'.
    """

    stats = SyncStats()

    # discover org folders
    try:
        items = dav.propfind(root_folder, depth=1)
    except Exception:
        stats.errors += 1
        raise

    org_dirs = [x for x in items if x.is_dir and x.name]
    if org_names_filter:
        wanted = {n.strip() for n in org_names_filter if n.strip()}
        org_dirs = [d for d in org_dirs if d.name in wanted]

    for d in org_dirs:
        stats.orgs_seen += 1
        org = _ensure_org(db, d.name, create_orgs)
        if not org:
            continue

        # Ensure org_artifacts exist (in case new org created)
        from app.api.web import _ensure_org_artifacts_materialized  # local import to avoid circular

        _ensure_org_artifacts_materialized(db, org.id)
        db.commit()

        # Map short_name -> list(org_artifact_id)
        oa_rows = (
            db.query(OrgArtifact, Artifact)
            .join(Artifact, Artifact.id == OrgArtifact.artifact_id)
            .filter(OrgArtifact.org_id == org.id)
            .all()
        )
        oa_by_sn: dict[str, list[OrgArtifact]] = {}
        item_no_by_oa_id: dict[int, int | None] = {}
        for (oa, a) in oa_rows:
            if not a.short_name:
                continue
            oa_by_sn.setdefault(a.short_name, []).append(oa)
            item_no_by_oa_id[oa.id] = a.achievement_item_no

        # Build a trie from ONLY short_names present for this org to limit traversal
        trie = _build_short_name_trie(sorted(oa_by_sn.keys()))
        org_root_folder = _join_path(root_folder, org.name)
        found = _iter_existing_files_for_known_short_names(dav=dav, org_root_folder=org_root_folder, trie=trie, stats=stats)

        # Group files per short_name to map them sequentially to пункты
        files_by_sn: dict[str, tuple[str, list[object]]] = {}
        for (sn, folder, f) in found:
            if sn not in files_by_sn:
                files_by_sn[sn] = (folder, [])
            files_by_sn[sn][1].append(f)

        for sn, (folder, files) in files_by_sn.items():
            oas = oa_by_sn.get(sn) or []
            if not oas:
                continue

            oas_sorted = _sort_org_artifacts_by_point(oas, item_no_by_oa_id)
            files_sorted = _sort_files_for_mapping(files)

            # Map file #1 -> пункт 1, file #2 -> пункт 2, ...
            mapped = min(len(oas_sorted), len(files_sorted))
            stats.files_seen += len(files_sorted)
            if len(files_sorted) > len(oas_sorted):
                # extra files exist in cloud for this short_name; skip them (MVP)
                stats.files_skipped += (len(files_sorted) - len(oas_sorted))

            for idx in range(mapped):
                oa = oas_sorted[idx]
                f = files_sorted[idx]
                remote_path = _join_path(folder, f.name)
                etag = _norm_etag(getattr(f, "etag", None))
                size = int(getattr(f, "size_bytes", None) or 0)

                # idempotency per org+remote_path
                state = (
                    db.query(NextcloudRemoteFileState)
                    .filter(NextcloudRemoteFileState.org_id == org.id, NextcloudRemoteFileState.remote_path == remote_path)
                    .one_or_none()
                )
                if state and _same_remote_file(state=state, etag=etag, size=size):
                    stats.files_skipped += 1
                    continue

                try:
                    content, content_type = dav.download(remote_path)
                except Exception:
                    stats.errors += 1
                    continue
                stats.files_downloaded += 1
                if len(content) > settings.max_upload_mb * 1024 * 1024:
                    stats.errors += 1
                    continue

                sha256 = hashlib.sha256(content).hexdigest()

                current_max = db.query(func.max(FileVersion.version_no)).filter(FileVersion.org_artifact_id == oa.id).scalar() or 0
                fv = FileVersion(
                    org_artifact_id=oa.id,
                    version_no=int(current_max) + 1,
                    original_filename=f.name,
                    content_type=content_type,
                    size_bytes=len(content),
                    sha256=sha256,
                    storage_backend="postgres",
                    storage_key=f"nextcloud:{remote_path}",
                    blob=content,
                    created_at=datetime.utcnow(),
                    created_by_user_id=actor.id,
                )
                db.add(fv)
                db.flush()
                stats.file_versions_created += 1

                before = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}
                oa.status = OrgArtifactStatus.uploaded
                oa.current_file_version_id = fv.id
                oa.updated_at = datetime.utcnow()
                oa.updated_by_user_id = actor.id
                after = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}

                write_audit_log(
                    db,
                    actor=actor,
                    org_id=org.id,
                    action="nextcloud_import",
                    entity_type="org_artifact",
                    entity_id=str(oa.id),
                    before=before,
                    after={
                        **after,
                        "remote_path": remote_path,
                        "etag": etag,
                    },
                    request=request,
                )

                # update remote state (keep the latest import)
                if not state:
                    state = NextcloudRemoteFileState(
                        org_id=org.id,
                        org_artifact_id=oa.id,
                        remote_path=remote_path,
                        etag=etag,
                        size_bytes=max(size, len(content)),
                        imported_file_version_id=fv.id,
                        imported_at=datetime.utcnow(),
                    )
                    db.add(state)
                else:
                    state.org_artifact_id = oa.id
                    state.etag = etag
                    state.size_bytes = max(size, len(content))
                    state.imported_file_version_id = fv.id
                    state.imported_at = datetime.utcnow()

                db.commit()

    return stats


def _upper(s: str) -> str:
    return (s or "").strip().upper()


def _short_name_prefixes(short_names: list[str]) -> set[str]:
    """
    For short_name like "НС.250.ИБ.10" create prefixes:
      - "НС"
      - "НС.250"
      - "НС.250.ИБ"
    We will use them to detect intermediate folders in the archive-like layout.
    """
    out: set[str] = set()
    for sn in short_names:
        parts = [p.strip() for p in (sn or "").split(".") if p.strip()]
        if len(parts) < 2:
            continue
        acc: list[str] = []
        for p in parts[:-1]:
            acc.append(p)
            out.add(".".join(acc).upper())
    return out


def _list_files_in_folder(dav: NextcloudDavClient, folder: str, *, stats: SyncStats) -> tuple[str, list[object]]:
    """
    Return (files_folder, files[]) for a given artifact folder.
    In real clouds, files may be stored directly in the folder OR under a 'files' subfolder.
    """
    stats.folders_seen += 1
    listing = dav.propfind(folder, depth=1)
    files = [it for it in listing if (not getattr(it, "is_dir", False)) and getattr(it, "name", "")]
    if files:
        return folder, files
    # Fallback: try "files" subfolder
    files_dir = next((it for it in listing if getattr(it, "is_dir", False) and str(getattr(it, "name", "")).lower() == "files"), None)
    if files_dir:
        sub = _join_path(folder, files_dir.name)
        stats.folders_seen += 1
        listing2 = dav.propfind(sub, depth=1)
        files2 = [it for it in listing2 if (not getattr(it, "is_dir", False)) and getattr(it, "name", "")]
        return sub, files2
    return folder, []


def _href_child_path(*, href: str, rel_root: str) -> str:
    """
    Convert DAV href -> relative child path inside rel_root.
    Example:
      rel_root = "ROOT/Org/03 Артефакты/05 СЗИ"
      href_path_dec endswith ".../ROOT/Org/03 Артефакты/05 СЗИ/СЗИ.2FA/СЗИ.2FA.1/a.pdf"
      => returns "СЗИ.2FA/СЗИ.2FA.1/a.pdf"
    """
    from urllib.parse import unquote, urlparse

    p = urlparse(href).path if "://" in (href or "") else (href or "")
    p = unquote((p or "").strip())
    p = p.rstrip("/")
    rel_root_norm = (rel_root or "").strip("/").strip()
    if not rel_root_norm:
        return p.strip("/")
    needle = "/" + rel_root_norm.strip("/")
    idx = p.rfind(needle)
    if idx == -1:
        # fallback: best-effort by suffix segments
        return p.split("/")[-1]
    tail = p[idx + len(needle) :].lstrip("/")
    return tail


def sync_from_nextcloud_v2(
    *,
    db: Session,
    actor: User,
    dav: NextcloudDavClient,
    root_folder: str,
    create_orgs: bool,
    org_names_filter: list[str] | None = None,
    request=None,
) -> SyncStats:
    """
    Sync artifacts from Nextcloud WebDAV (V2).

    Expected folder layout (as in docs/03 Артефакты.zip):
      <root>/<OrgName>/03 Артефакты/<category>/<short_name folder>/[files]
    where:
      - category: e.g. "05 СЗИ"
      - short_name folder may be:
          - exact short_name, e.g. "МНТ.SIEM.1"
          - prefix folder, e.g. "СЗИ.2FA", containing children "СЗИ.2FA.1", "СЗИ.2FA.2", ...
    """

    stats = SyncStats()

    # discover org folders
    try:
        items = dav.propfind(root_folder, depth=1)
    except Exception:
        stats.errors += 1
        raise

    org_dirs = [x for x in items if x.is_dir and x.name]
    if org_names_filter:
        wanted = {n.strip() for n in org_names_filter if n.strip()}
        org_dirs = [d for d in org_dirs if d.name in wanted]

    for d in org_dirs:
        stats.orgs_seen += 1
        org = _ensure_org(db, d.name, create_orgs)
        if not org:
            continue

        # Ensure org_artifacts exist (in case new org created)
        from app.api.web import _ensure_org_artifacts_materialized  # local import to avoid circular

        _ensure_org_artifacts_materialized(db, org.id)
        db.commit()

        # Map short_name -> list(org_artifact_id)
        oa_rows = (
            db.query(OrgArtifact, Artifact)
            .join(Artifact, Artifact.id == OrgArtifact.artifact_id)
            .filter(OrgArtifact.org_id == org.id)
            .all()
        )
        oa_by_sn: dict[str, list[OrgArtifact]] = {}
        item_no_by_oa_id: dict[int, int | None] = {}
        for (oa, a) in oa_rows:
            if not a.short_name:
                continue
            oa_by_sn.setdefault(a.short_name, []).append(oa)
            item_no_by_oa_id[oa.id] = a.achievement_item_no

        known_sns = sorted(oa_by_sn.keys())
        known_upper_to_sn = {_upper(sn): sn for sn in known_sns}
        prefixes = _short_name_prefixes(known_sns)

        org_root_folder = _join_path(root_folder, org.name, "03 Артефакты")

        # Traverse: categories -> (short_name | prefix) -> optional children -> files
        try:
            categories = dav.propfind(org_root_folder, depth=1)
        except Exception:
            # no folder / access denied -> skip this org
            continue
        stats.folders_seen += 1

        # Collect files per short_name for sequential point mapping
        # Store pairs (remote_path, DavItem)
        files_by_sn: dict[str, list[tuple[str, object]]] = {}

        for cat in categories:
            if not getattr(cat, "is_dir", False) or not getattr(cat, "name", ""):
                continue
            cat_folder = _join_path(org_root_folder, cat.name)
            # Perf: one PROPFIND per category with depth=4, then map files by href path.
            # This avoids N requests for N (empty) artifact folders.
            try:
                deep = dav.propfind(cat_folder, depth=4)
            except Exception:
                # fallback to shallow scan (old behavior) if server rejects large Depth
                try:
                    deep = dav.propfind(cat_folder, depth=1)
                except Exception:
                    continue
            stats.folders_seen += 1

            for it in deep:
                if getattr(it, "is_dir", False):
                    continue
                name = str(getattr(it, "name", "") or "")
                if not name:
                    continue

                rel_child = _href_child_path(href=str(getattr(it, "href", "") or ""), rel_root=cat_folder)
                segs = [s for s in (rel_child or "").split("/") if s]
                if not segs:
                    continue

                # find the first segment that matches a known short_name
                matched_sn: str | None = None
                sn_folder_rel_parts: list[str] = []
                for idx_seg, seg in enumerate(segs):
                    seg_u = _upper(seg)
                    if seg_u in known_upper_to_sn:
                        matched_sn = known_upper_to_sn[seg_u]
                        sn_folder_rel_parts = segs[: idx_seg + 1]
                        break
                    # skip obvious category names; ignore other folders unless they are a known prefix
                    if idx_seg == 0:
                        continue
                    if seg_u in prefixes:
                        continue
                if not matched_sn:
                    continue

                # remote_path should be full path to the file inside category
                remote_path = _join_path(cat_folder, *segs)
                files_by_sn.setdefault(matched_sn, []).append((remote_path, it))

        # Import per short_name and map to points
        for sn, file_pairs in files_by_sn.items():
            oas = oa_by_sn.get(sn) or []
            if not oas:
                continue

            oas_sorted = _sort_org_artifacts_by_point(oas, item_no_by_oa_id)
            items = [it for (_rp, it) in file_pairs]
            files_sorted = _sort_files_for_mapping(items)
            # Build remote_path map by object identity (ok because DavItem objects are unique per response)
            remote_by_id = {id(it): rp for (rp, it) in file_pairs}

            mapped = min(len(oas_sorted), len(files_sorted))
            stats.files_seen += len(files_sorted)
            if len(files_sorted) > len(oas_sorted):
                stats.files_skipped += (len(files_sorted) - len(oas_sorted))

            for idx in range(mapped):
                oa = oas_sorted[idx]
                f = files_sorted[idx]
                remote_path = remote_by_id.get(id(f)) or _join_path(cat_folder, f.name)
                etag = _norm_etag(getattr(f, "etag", None))
                size = int(getattr(f, "size_bytes", None) or 0)

                state = (
                    db.query(NextcloudRemoteFileState)
                    .filter(NextcloudRemoteFileState.org_id == org.id, NextcloudRemoteFileState.remote_path == remote_path)
                    .one_or_none()
                )
                if state and _same_remote_file(state=state, etag=etag, size=size):
                    stats.files_skipped += 1
                    continue

                try:
                    content, content_type = dav.download(remote_path)
                except Exception:
                    stats.errors += 1
                    continue
                stats.files_downloaded += 1
                if len(content) > settings.max_upload_mb * 1024 * 1024:
                    stats.errors += 1
                    continue

                sha256 = hashlib.sha256(content).hexdigest()

                current_max = db.query(func.max(FileVersion.version_no)).filter(FileVersion.org_artifact_id == oa.id).scalar() or 0
                fv = FileVersion(
                    org_artifact_id=oa.id,
                    version_no=int(current_max) + 1,
                    original_filename=f.name,
                    content_type=content_type,
                    size_bytes=len(content),
                    sha256=sha256,
                    storage_backend="postgres",
                    storage_key=f"nextcloud:{remote_path}",
                    blob=content,
                    created_at=datetime.utcnow(),
                    created_by_user_id=actor.id,
                )
                db.add(fv)
                db.flush()
                stats.file_versions_created += 1

                before = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}
                oa.status = OrgArtifactStatus.uploaded
                oa.current_file_version_id = fv.id
                oa.updated_at = datetime.utcnow()
                oa.updated_by_user_id = actor.id
                after = {"status": oa.status.value, "current_file_version_id": oa.current_file_version_id}

                write_audit_log(
                    db,
                    actor=actor,
                    org_id=org.id,
                    action="nextcloud_import_v2",
                    entity_type="org_artifact",
                    entity_id=str(oa.id),
                    before=before,
                    after={
                        **after,
                        "remote_path": remote_path,
                        "etag": etag,
                    },
                    request=request,
                )

                if not state:
                    state = NextcloudRemoteFileState(
                        org_id=org.id,
                        org_artifact_id=oa.id,
                        remote_path=remote_path,
                        etag=etag,
                        size_bytes=max(size, len(content)),
                        imported_file_version_id=fv.id,
                        imported_at=datetime.utcnow(),
                    )
                    db.add(state)
                else:
                    state.org_artifact_id = oa.id
                    state.etag = etag
                    state.size_bytes = max(size, len(content))
                    state.imported_file_version_id = fv.id
                    state.imported_at = datetime.utcnow()

                db.commit()

    return stats
