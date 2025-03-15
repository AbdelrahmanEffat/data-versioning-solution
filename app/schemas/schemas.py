# app/schemas/schemas.py
from pydantic import BaseModel, EmailStr, Field, validator
from typing import Dict, List, Optional, Any
from datetime import datetime
from uuid import UUID

class SchemaDefinition(BaseModel):
    column_name: str
    data_type: str
    is_nullable: bool = True
    description: Optional[str] = None

class DatasetCreate(BaseModel):
    dataset_name: str
    description: Optional[str] = None
    schema_definition: List[SchemaDefinition]
    data: List[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]] = None

    @validator('data')
    def validate_data_against_schema(cls, v, values):
        if 'schema_definition' in values:
            schema_columns = {col.column_name for col in values['schema_definition']}
            for row in v:
                if not all(key in schema_columns for key in row.keys()):
                    raise ValueError("Data columns must match schema definition")
        return v

class DatasetResponse(BaseModel):
    dataset_id: UUID
    dataset_name: str
    description: Optional[str]
    created_by: UUID
    created_at: datetime
    #metadata: Optional[Dict[str, Any]]
    dataset_metadata: Optional[Dict[str, Any]]

    class Config:
        from_attributes = True

class VersionCreate(BaseModel):
    data: List[Dict[str, Any]]
    schema_changes: Optional[List[SchemaDefinition]] = None
    comment: Optional[str] = None
    change_type: str = Field(..., pattern='^(INSERT|UPDATE|DELETE)$')

class VersionResponse(BaseModel):
    version_id: UUID
    version_number: str
    dataset_id: UUID
    created_at: datetime
    created_by: UUID
    change_type: str
    #comment: Optional[str]
    
    class Config:
        from_attributes = True

class VersionInfo(BaseModel):
    version_id: UUID
    version_number: str
    created_at: datetime
    created_by: UUID
    change_type: str
    comment: Optional[str]
    schema_definition: List[SchemaDefinition]
    complete_data: Optional[List[Dict[str, Any]]] 
    
    class Config:
        from_attributes = True

# In schemas.py
class VersionListInfo(BaseModel):
    version_id: UUID
    version_number: str
    created_at: datetime
    created_by: UUID
    change_type: str
    comment: Optional[str]
    schema_definition: List[SchemaDefinition]
    
    class Config:
        from_attributes = True

class VersionList(BaseModel):
    versions: List[VersionListInfo]  # Use VersionListInfo instead of VersionInfo
    total_count: int
    
    class Config:
        from_attributes = True
# Authentication schemas remain the same but with UUID for id
class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8)

class UserOut(BaseModel):
    id: UUID
    email: EmailStr
    created_at: datetime

    class Config:
        from_attributes = True

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

class TokenData(BaseModel):
    id: Optional[UUID] = None