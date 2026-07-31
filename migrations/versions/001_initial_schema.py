"""Initial database schema for Nanobot.

Revision ID: initial_schema
Revises: 
Create Date: 2024-01-01 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'initial_schema'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create initial database schema."""
    
    # Create sessions table
    op.create_table(
        'sessions',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('session_id', sa.String(length=255), nullable=False),
        sa.Column('user_id', sa.String(length=255), nullable=True),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('expires_at', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('data', sa.JSON(), nullable=True, default={}),
        sa.Column('metadata', sa.JSON(), nullable=True, default={}),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('session_id')
    )
    
    # Create index on session_id
    op.create_index('idx_sessions_session_id', 'sessions', ['session_id'])
    op.create_index('idx_sessions_expires_at', 'sessions', ['expires_at'])
    
    # Create conversation history table
    op.create_table(
        'conversation_history',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('session_id', sa.String(length=255), nullable=False),
        sa.Column('role', sa.String(length=50), nullable=False),
        sa.Column('content', sa.Text(), nullable=True),
        sa.Column('tool_calls', sa.JSON(), nullable=True),
        sa.Column('tool_results', sa.JSON(), nullable=True),
        sa.Column('metadata', sa.JSON(), nullable=True, default={}),
        sa.Column('created_at', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['session_id'], ['sessions.session_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes for conversation history
    op.create_index('idx_history_session_id', 'conversation_history', ['session_id'])
    op.create_index('idx_history_created_at', 'conversation_history', ['created_at'])
    
    # Create metrics table
    op.create_table(
        'metrics',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('metric_name', sa.String(length=100), nullable=False),
        sa.Column('metric_value', sa.Float(), nullable=False),
        sa.Column('labels', sa.JSON(), nullable=True, default={}),
        sa.Column('timestamp', sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create index for metrics
    op.create_index('idx_metrics_name_timestamp', 'metrics', ['metric_name', sa.desc('timestamp')])


def downgrade() -> None:
    """Drop initial database schema."""
    op.drop_index('idx_metrics_name_timestamp', table_name='metrics')
    op.drop_table('metrics')
    
    op.drop_index('idx_history_created_at', table_name='conversation_history')
    op.drop_index('idx_history_session_id', table_name='conversation_history')
    op.drop_table('conversation_history')
    
    op.drop_index('idx_sessions_expires_at', table_name='sessions')
    op.drop_index('idx_sessions_session_id', table_name='sessions')
    op.drop_table('sessions')
