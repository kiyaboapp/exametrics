"""First Real Migrations

Revision ID: 0c50564a0e1c
Revises: 5f16c0e4e125
Create Date: 2025-07-20 10:16:59.686755

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0c50564a0e1c'
down_revision: Union[str, Sequence[str], None] = '5f16c0e4e125'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
