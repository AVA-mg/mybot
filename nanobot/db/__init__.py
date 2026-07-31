"""
Database configuration and session management for Nanobot.

This module provides async database engine setup and session factories.
"""

import os
from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)
from sqlalchemy.pool import NullPool

from .models import Base, UserModel, SessionModel, MemoryModel, CronJobModel


class DatabaseConfig:
    """Database configuration manager."""

    def __init__(self):
        self.engine: AsyncEngine | None = None
        self.async_session_factory: async_sessionmaker[AsyncSession] | None = None

    def initialize(self, database_url: str | None = None) -> None:
        """
        Initialize the database engine and session factory.

        Args:
            database_url: PostgreSQL connection URL. If None, reads from environment.
        """
        if database_url is None:
            database_url = os.getenv(
                "DATABASE_URL",
                "postgresql+asyncpg://nanobot:nanobot@localhost:5432/nanobot"
            )

        # Create async engine with connection pool settings
        self.engine = create_async_engine(
            database_url,
            echo=os.getenv("DB_ECHO", "false").lower() == "true",
            pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
            pool_pre_ping=True,
            pool_recycle=3600,
        )

        # Create async session factory
        self.async_session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )

    async def create_tables(self) -> None:
        """Create all database tables."""
        if self.engine is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def drop_tables(self) -> None:
        """Drop all database tables."""
        if self.engine is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

    async def close(self) -> None:
        """Close the database engine."""
        if self.engine:
            await self.engine.dispose()
            self.engine = None
            self.async_session_factory = None


# Global database instance
db_config = DatabaseConfig()


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency to get a database session.

    Yields:
        AsyncSession: Database session instance.
    """
    if db_config.async_session_factory is None:
        db_config.initialize()

    async with db_config.async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_database(database_url: str | None = None) -> None:
    """
    Initialize the database and create tables.

    Args:
        database_url: Optional PostgreSQL connection URL.
    """
    db_config.initialize(database_url)
    await db_config.create_tables()


__all__ = [
    "Base",
    "UserModel",
    "SessionModel",
    "MemoryModel",
    "CronJobModel",
    "DatabaseConfig",
    "db_config",
    "get_db_session",
    "init_database",
]
