"""More Changes

Revision ID: 6fb6c74c824d
Revises: c6789fa3b288
Create Date: 2025-07-18 13:35:13.626162

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6fb6c74c824d'
down_revision: Union[str, Sequence[str], None] = 'c6789fa3b288'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
