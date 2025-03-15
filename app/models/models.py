# app/models/models.py
from sqlalchemy import Column, String, DateTime, ForeignKey, Boolean, JSON, func
from sqlalchemy.types import Enum
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
from ..core.database import Base

class User(Base):
    __tablename__ = "users"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    email = Column(String, unique=True, nullable=False)
    password = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())

    def __repr__(self):
        return f"<User(email={self.email})>"

class Dataset(Base):
    __tablename__ = 'datasets'
    dataset_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    dataset_name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    created_by = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    dataset_metadata = Column(JSON, nullable=True) ###########

    # Relationships
    versions = relationship("Version", back_populates="dataset")

    def __repr__(self):
        return f"<Dataset(dataset_name={self.dataset_name})>"

class Version(Base):
    __tablename__ = 'versions'
    version_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    dataset_id = Column(UUID(as_uuid=True), ForeignKey('datasets.dataset_id'), nullable=False)
    version_number = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    created_by = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    change_type = Column(Enum('INSERT', 'UPDATE', 'DELETE', name='change_type_enum'), nullable=True)
    comment = Column(String, nullable=True)

    # Relationships
    dataset = relationship("Dataset", back_populates="versions")
    schema_versions = relationship("SchemaVersion", back_populates="version")
    changes = relationship("ChangeLog", back_populates="version")

    def __repr__(self):
        return f"<Version(version_number={self.version_number}, dataset_id={self.dataset_id})>"

class SchemaVersion(Base):
    __tablename__ = 'schema_versions'
    schema_version_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    version_id = Column(UUID(as_uuid=True), ForeignKey('versions.version_id'), nullable=False)
    column_name = Column(String, nullable=False)
    data_type = Column(String, nullable=False)
    is_nullable = Column(Boolean, default=True)
    description = Column(String, nullable=True)

    # Relationships
    version = relationship("Version", back_populates="schema_versions")

    def __repr__(self):
        return f"<SchemaVersion(column_name={self.column_name}, data_type={self.data_type})>"

class ChangeLog(Base):
    __tablename__ = 'change_log'
    change_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False)
    version_id = Column(UUID(as_uuid=True), ForeignKey('versions.version_id'), nullable=False)
    changed_by = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    operation_type = Column(Enum('INSERT', 'UPDATE', 'DELETE', name='operation_enum'), nullable=False)
    operation_time = Column(DateTime, nullable=False, server_default=func.now())
    changed_data = Column(JSON, nullable=False)

    # Relationships
    version = relationship("Version", back_populates="changes")

    def __repr__(self):
        return f"<ChangeLog(operation_type={self.operation_type}, version_id={self.version_id})>"
