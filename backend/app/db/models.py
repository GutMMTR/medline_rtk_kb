from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, Float, ForeignKey, Integer, LargeBinary, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Role(str, enum.Enum):
    customer = "customer"
    auditor = "auditor"
    admin = "admin"


class OrgArtifactStatus(str, enum.Enum):
    missing = "missing"
    uploaded = "uploaded"


class Organization(Base):
    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    # Кто/как создал организацию (для UI-диагностики: вручную админом или через синхронизацию).
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_via: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")  # manual|nextcloud|system

    created_by: Mapped["User | None"] = relationship(foreign_keys=[created_by_user_id])


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    login: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    # При удалении пользователя должны удаляться и его membership'ы (иначе ORM пытается проставить NULL в user_id).
    memberships: Mapped[list["UserOrgMembership"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class UserOrgMembership(Base):
    __tablename__ = "user_org_memberships"
    __table_args__ = (UniqueConstraint("user_id", "org_id", name="uq_user_org"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    org_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)
    role: Mapped[Role] = mapped_column(Enum(Role, name="role"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    user: Mapped[User] = relationship(back_populates="memberships")
    org: Mapped[Organization] = relationship()


class ArtifactNode(Base):
    __tablename__ = "artifact_nodes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    parent_id: Mapped[int | None] = mapped_column(ForeignKey("artifact_nodes.id", ondelete="CASCADE"), nullable=True, index=True)

    segment: Mapped[str] = mapped_column(String(255), nullable=False)
    full_path: Mapped[str] = mapped_column(String(2048), nullable=False, unique=True, index=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    parent: Mapped["ArtifactNode | None"] = relationship(remote_side="ArtifactNode.id", back_populates="children")
    children: Mapped[list["ArtifactNode"]] = relationship(back_populates="parent", cascade="all, delete-orphan")
    artifact: Mapped["Artifact | None"] = relationship(back_populates="node", uselist=False)


class Artifact(Base):
    __tablename__ = "artifacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    node_id: Mapped[int] = mapped_column(ForeignKey("artifact_nodes.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)

    # Может быть пустым в импорте (тогда multiple NULL допустимы в Postgres).
    artifact_key: Mapped[str | None] = mapped_column(String(255), nullable=True, unique=True, index=True)
    # Поля из Excel (лист "Программа") для фильтров в UI.
    topic: Mapped[str] = mapped_column(String(255), nullable=False, default="", index=True)
    domain: Mapped[str] = mapped_column(String(255), nullable=False, default="", index=True)
    indicator_name: Mapped[str] = mapped_column(Text, nullable=False, default="")
    short_name: Mapped[str] = mapped_column(String(255), nullable=False, default="", index=True)
    kb_level: Mapped[str] = mapped_column(String(64), nullable=False, default="", index=True)

    achievement_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    achievement_item_no: Mapped[int | None] = mapped_column(Integer, nullable=True)
    achievement_item_text: Mapped[str] = mapped_column(Text, nullable=False, default="")

    title: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    node: Mapped[ArtifactNode] = relationship(back_populates="artifact")


class OrgArtifact(Base):
    __tablename__ = "org_artifacts"
    __table_args__ = (UniqueConstraint("org_id", "artifact_id", name="uq_org_artifact"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    artifact_id: Mapped[int] = mapped_column(ForeignKey("artifacts.id", ondelete="CASCADE"), nullable=False, index=True)

    status: Mapped[OrgArtifactStatus] = mapped_column(Enum(OrgArtifactStatus, name="org_artifact_status"), nullable=False, default=OrgArtifactStatus.missing)
    current_file_version_id: Mapped[int | None] = mapped_column(ForeignKey("file_versions.id", ondelete="SET NULL"), nullable=True)

    # Аудит (к какой версии относится "проверено" + кто/когда)
    audited_file_version_id: Mapped[int | None] = mapped_column(ForeignKey("file_versions.id", ondelete="SET NULL"), nullable=True)
    audited_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    audited_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    org: Mapped[Organization] = relationship()
    artifact: Mapped[Artifact] = relationship()
    current_file_version: Mapped["FileVersion | None"] = relationship(foreign_keys=[current_file_version_id], post_update=True)
    audited_file_version: Mapped["FileVersion | None"] = relationship(foreign_keys=[audited_file_version_id], post_update=True)
    audited_by: Mapped["User | None"] = relationship(foreign_keys=[audited_by_user_id])
    # Между org_artifacts и file_versions есть два FK-пути (org_artifact_id и current_file_version_id),
    # поэтому явно указываем, какой FK использовать для коллекции версий.
    versions: Mapped[list["FileVersion"]] = relationship(
        back_populates="org_artifact",
        cascade="all, delete-orphan",
        foreign_keys="FileVersion.org_artifact_id",
    )


class FileVersion(Base):
    __tablename__ = "file_versions"
    __table_args__ = (UniqueConstraint("org_artifact_id", "version_no", name="uq_org_artifact_version"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_artifact_id: Mapped[int] = mapped_column(ForeignKey("org_artifacts.id", ondelete="CASCADE"), nullable=False, index=True)
    version_no: Mapped[int] = mapped_column(Integer, nullable=False)

    original_filename: Mapped[str] = mapped_column(String(1024), nullable=False)
    content_type: Mapped[str] = mapped_column(String(255), nullable=False, default="application/octet-stream")
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    storage_backend: Mapped[str] = mapped_column(String(64), nullable=False, default="postgres")
    storage_key: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    blob: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    org_artifact: Mapped[OrgArtifact] = relationship(
        back_populates="versions",
        foreign_keys=[org_artifact_id],
    )


class FilePreview(Base):
    __tablename__ = "file_previews"
    __table_args__ = (UniqueConstraint("file_version_id", name="uq_file_previews_file_version_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    file_version_id: Mapped[int] = mapped_column(ForeignKey("file_versions.id", ondelete="CASCADE"), nullable=False, index=True)

    preview_mime: Mapped[str] = mapped_column(String(255), nullable=False, default="application/pdf")
    preview_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    preview_sha256: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    preview_blob: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    last_error: Mapped[str] = mapped_column(Text, nullable=False, default="")
    last_error_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class OrgArtifactComment(Base):
    __tablename__ = "org_artifact_comments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    org_artifact_id: Mapped[int] = mapped_column(ForeignKey("org_artifacts.id", ondelete="CASCADE"), nullable=False, index=True)
    author_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)

    comment_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False, index=True)

    org: Mapped[Organization] = relationship()
    org_artifact: Mapped[OrgArtifact] = relationship()
    author: Mapped["User | None"] = relationship()


class AuditLog(Base):
    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False, index=True)

    actor_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    org_id: Mapped[int | None] = mapped_column(ForeignKey("organizations.id", ondelete="SET NULL"), nullable=True, index=True)

    action: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    entity_type: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    entity_id: Mapped[str] = mapped_column(String(255), nullable=False)

    before_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    after_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    ip: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    user_agent: Mapped[str] = mapped_column(String(1024), nullable=False, default="")


class StoredFile(Base):
    """
    Упрощённая модель для MVP: файл привязан к организации.
    Позже добавим артефакты/версии/аудит иерархии.
    """

    __tablename__ = "stored_files"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)

    original_filename: Mapped[str] = mapped_column(String(1024), nullable=False)
    content_type: Mapped[str] = mapped_column(String(255), nullable=False, default="application/octet-stream")
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    blob: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    created_by_user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    note: Mapped[str] = mapped_column(Text, nullable=False, default="")


class NextcloudIntegrationSettings(Base):
    __tablename__ = "nextcloud_integration_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    base_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")  # e.g. https://nextcloud.soc.rt.ru
    username: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    password: Mapped[str] = mapped_column(String(255), nullable=False, default="")  # MVP: хранение в БД (лучше app-password)

    root_folder: Mapped[str] = mapped_column(String(1024), nullable=False, default="")  # relative in WebDAV, e.g. "" or "Artifacts"
    create_orgs: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str] = mapped_column(Text, nullable=False, default="")


class NextcloudRemoteFileState(Base):
    __tablename__ = "nextcloud_remote_file_state"
    __table_args__ = (
        UniqueConstraint("org_id", "remote_path", name="uq_nextcloud_org_remote_path"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    org_artifact_id: Mapped[int] = mapped_column(ForeignKey("org_artifacts.id", ondelete="CASCADE"), nullable=False, index=True)

    remote_path: Mapped[str] = mapped_column(String(2048), nullable=False)
    etag: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    imported_file_version_id: Mapped[int | None] = mapped_column(ForeignKey("file_versions.id", ondelete="SET NULL"), nullable=True)
    imported_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    org: Mapped[Organization] = relationship()
    org_artifact: Mapped[OrgArtifact] = relationship()
    imported_file_version: Mapped[FileVersion | None] = relationship()


class IndexKbManualValue(Base):
    """
    Ручные значения для упрощённого UI Индекса КБ (например, лист "Управление ИБ").
    Для строк, где short_name отсутствует в справочнике артефактов, значение вводится вручную.
    """

    __tablename__ = "index_kb_manual_values"
    __table_args__ = (
        UniqueConstraint("org_id", "sheet_name", "row_key", name="uq_index_kb_manual_org_sheet_row"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    sheet_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    row_key: Mapped[str] = mapped_column(String(255), nullable=False, default="")  # stable identifier for the row in template

    value: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)  # 0..5 (может быть дробным)

    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    org: Mapped[Organization] = relationship()
    updated_by: Mapped["User | None"] = relationship(foreign_keys=[updated_by_user_id])


class IndexKbTemplateRow(Base):
    """
    Структура строк "Индекс КБ" (группы/пункты) по листам.

    Важно: это НЕ значения по организации, а именно "шаблон", который загружается один раз
    (из Excel-эталона или иного источника) и дальше используется UI/выгрузками без парсинга .xlsx.
    """

    __tablename__ = "index_kb_template_rows"
    __table_args__ = (
        UniqueConstraint("sheet_name", "row_key", name="uq_index_kb_template_sheet_row"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sheet_name: Mapped[str] = mapped_column(String(255), nullable=False, default="", index=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0, index=True)

    kind: Mapped[str] = mapped_column(String(32), nullable=False, default="item")  # group|item
    row_key: Mapped[str] = mapped_column(String(255), nullable=False, default="")  # stable id (e.g. "СЗИ.X.Y" / "group:ABC")

    title: Mapped[str] = mapped_column(Text, nullable=False, default="")
    short_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    group_code: Mapped[str] = mapped_column(String(255), nullable=False, default="")
