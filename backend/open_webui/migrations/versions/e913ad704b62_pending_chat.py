"""Add atomic pending-conversation slots."""
from alembic import op
import sqlalchemy as sa

revision = 'e913ad704b62'
down_revision = 'c4e1a7d93b20'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('pending_chat',
        sa.Column('slot', sa.String(), primary_key=True),
        sa.Column('chat_id', sa.String(), nullable=False, unique=True),
        sa.Column('user_id', sa.String(), nullable=False),
        sa.Column('machine_id', sa.String(), nullable=False),
        sa.Column('state', sa.String(), nullable=False),
        sa.Column('expires_at', sa.BigInteger(), nullable=False))
    op.create_index('ix_pending_chat_expires_at', 'pending_chat', ['expires_at'])


def downgrade():
    op.drop_table('pending_chat')
