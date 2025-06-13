# file: db.py

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Date,
    Boolean,
    DateTime,
    Text,
    ForeignKey
)
from sqlalchemy.orm import sessionmaker, declarative_base, relationship

Base = declarative_base()

class Profile(Base):
    __tablename__ = 'profiles'
    profile_id      = Column(Integer, primary_key=True, index=True)
    slug            = Column(String, unique=True, index=True, nullable=False)
    profile_url     = Column(Text, nullable=False)
    name            = Column(String)
    image_url       = Column(Text)
    source_name     = Column(String)
    source_url      = Column(Text)
    author          = Column(String)
    description_html= Column(Text)
    description_text= Column(Text)
    region          = Column(String)
    country         = Column(String)
    state           = Column(String)
    sex             = Column(String)
    date_of_killing = Column(Date)
    previous_threats= Column(Boolean)
    type_of_work    = Column(String)
    sector          = Column(String)
    sector_detail   = Column(Text)        # JSON-encoded list
    more_information= Column(String)
    contact_email   = Column(String)
    created_at      = Column(DateTime)
    urls            = relationship('URL', back_populates='profile')


class URL(Base):
    __tablename__ = 'urls'
    url_id         = Column(Integer, primary_key=True, index=True)
    profile_id     = Column(Integer, ForeignKey('profiles.profile_id'), nullable=False)
    label          = Column(String)
    url            = Column(Text, nullable=False)
    # Phase I fields
    is_archived    = Column(Boolean)
    archived_url   = Column(Text)
    # Phase II fields (validation & scraping)
    is_active      = Column(Boolean,  nullable=True)
    contains_name  = Column(Boolean,  nullable=True)
    page_text      = Column(Text,     nullable=True)
    checked_at     = Column(DateTime, nullable=True)

    profile = relationship('Profile', back_populates='urls')


def get_engine(db_url: str, echo: bool = False):
    return create_engine(db_url, echo=echo, future=True)


def get_session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)


def init_db(db_url: str, echo: bool = False):
    """
    Create database engine, tables, and return session factory.
    """
    engine = get_engine(db_url, echo=echo)
    Base.metadata.create_all(engine)
    return get_session_factory(engine)
