from datetime import datetime
from sqlalchemy import create_engine, func
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    sessionmaker,
    declared_attr,
)
import re
import os

# 1. Define the path explicitly
DB_FOLDER = "./data"
DB_FILE = "weather.db"

# 2. Check if folder exists, if not, create it
if not os.path.exists(DB_FOLDER):
    os.makedirs(DB_FOLDER)

# 3. Database File Location
DATABASE_URL = f"sqlite:///{DB_FOLDER}/{DB_FILE}"

# 4. Create the Engine
engine = create_engine(
    DATABASE_URL, echo=True, connect_args={"check_same_thread": False}
)


# 5. Define the Base for your models (ORM)
class Base(DeclarativeBase):
    """Base class for all ORM"""

    @declared_attr.directive
    def __tablename__(cls):
        return re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).lower()

    # audit columns
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        server_default=func.now(),
        onupdate=func.now(),  # Auto-updates on any change
    )

    # primary key column for all tables
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)


# 6. Create the Session Factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
