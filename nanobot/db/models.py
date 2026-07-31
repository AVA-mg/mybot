"""
Database models for Nanobot.

This module defines SQLAlchemy ORM models for:
- users: User accounts and profiles
- sessions: Conversation sessions with state
- memories: Long-term memory storage
- cron_jobs: Scheduled tasks
"""

from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
    JSON,
    Index,
    Boolean,
)
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class UserModel(AsyncAttrs, Base):
    """User account model."""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    metadata_ = Column("metadata", JSON, default=dict, nullable=False)

    # Relationships
    sessions = relationship("SessionModel", back_populates="user", cascade="all, delete-orphan")
    memories = relationship("MemoryModel", back_populates="user", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_users_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<User(id={self.id}, username='{self.username}')>"


class SessionModel(AsyncAttrs, Base):
    """Conversation session model."""

    __tablename__ = "sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(255), unique=True, nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True)
    bot_id = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_activity = Column(DateTime, default=datetime.utcnow, nullable=False)
    message_count = Column(Integer, default=0, nullable=False)
    state = Column(JSON, default=dict, nullable=False)
    context = Column(JSON, default=list, nullable=False)
    metadata_ = Column("metadata", JSON, default=dict, nullable=False)

    # Relationships
    user = relationship("UserModel", back_populates="sessions")

    __table_args__ = (
        Index("ix_sessions_last_activity", "last_activity"),
        Index("ix_sessions_bot_id", "bot_id"),
    )

    def __repr__(self) -> str:
        return f"<Session(id={self.id}, key='{self.key}', user_id={self.user_id})>"


class MemoryModel(AsyncAttrs, Base):
    """Long-term memory storage model."""

    __tablename__ = "memories"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True)
    session_key = Column(String(255), ForeignKey("sessions.key", ondelete="CASCADE"), nullable=True, index=True)
    content = Column(Text, nullable=False)
    embedding = Column(JSON, nullable=True)  # Vector embedding stored as JSON array
    category = Column(String(100), nullable=True, index=True)
    importance = Column(Integer, default=1, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    accessed_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    metadata_ = Column("metadata", JSON, default=dict, nullable=False)

    # Relationships
    user = relationship("UserModel", back_populates="memories")
    session = relationship("SessionModel", back_populates="memories")

    __table_args__ = (
        Index("ix_memories_category_importance", "category", "importance"),
        Index("ix_memories_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<Memory(id={self.id}, category='{self.category}', importance={self.importance})>"


class CronJobModel(AsyncAttrs, Base):
    """Scheduled task model."""

    __tablename__ = "cron_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    bot_id = Column(String(255), nullable=False, index=True)
    schedule = Column(String(100), nullable=False)  # Cron expression
    enabled = Column(Boolean, default=True, nullable=False)
    last_run = Column(DateTime, nullable=True)
    next_run = Column(DateTime, nullable=True)
    run_count = Column(Integer, default=0, nullable=False)
    last_status = Column(String(50), nullable=True)  # success, failed, running
    last_error = Column(Text, nullable=True)
    config = Column(JSON, default=dict, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("ix_cron_jobs_next_run", "next_run"),
        Index("ix_cron_jobs_enabled", "enabled"),
    )

    def __repr__(self) -> str:
        return f"<CronJob(id={self.id}, name='{self.name}', enabled={self.enabled})>"
