# app/api/endpoints/versions.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from uuid import UUID
from ...core.database import get_db
from ...models import models
from ...schemas import schemas
from ...utils.oauth2 import get_current_user

router = APIRouter(
    prefix="/datasets",
    tags=['Datasets']
)

@router.post("/", status_code=status.HTTP_201_CREATED, response_model=schemas.DatasetResponse)
async def create_dataset(
    dataset: schemas.DatasetCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """Create a new dataset with initial version"""
    new_dataset = models.Dataset(
        dataset_name=dataset.dataset_name,
        description=dataset.description,
        created_by=current_user.id,
        dataset_metadata=dataset.metadata or None #############
    )
    db.add(new_dataset)
    db.flush()

    # Create initial version
    initial_version = models.Version(
        dataset_id=new_dataset.dataset_id,
        version_number="1.0",
        created_by=current_user.id,
        change_type="INSERT"
    )
    db.add(initial_version)
    db.flush()

    # Add schema information
    for column in dataset.schema_definition:
        schema_version = models.SchemaVersion(
            version_id=initial_version.version_id,
            **column.dict()
        )
        db.add(schema_version)

    # Record initial data
    change_log = models.ChangeLog(
        version_id=initial_version.version_id,
        changed_by=current_user.id,
        operation_type="INSERT",
        changed_data={"initial_data": dataset.data}
    )
    db.add(change_log)

    db.commit()
    db.refresh(new_dataset)
    return new_dataset

@router.get("/{dataset_id}/latest", response_model=schemas.VersionInfo)
async def get_latest_version(
    dataset_id: UUID,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """Get the latest version of a dataset"""
    latest_version = (
        db.query(models.Version)
        .filter(models.Version.dataset_id == dataset_id)
        .order_by(models.Version.created_at.desc())
        .first()
    )
    
    if not latest_version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with id {dataset_id} not found"
        )
    
    # Get schema for latest version
    schema = (
        db.query(models.SchemaVersion)
        .filter(models.SchemaVersion.version_id == latest_version.version_id)
        .all()
    )
    
    # Fetch all change logs up to and including the latest version
    change_logs = (
        db.query(models.ChangeLog)
        .join(models.Version)
        .filter(
            models.Version.dataset_id == dataset_id,
            models.Version.created_at <= latest_version.created_at  # Only changes up to the latest version
        )
        .order_by(models.Version.created_at.asc())
        .all()
    )

        # Start with the initial data
    complete_data = []
    for change_log in change_logs:
        changed_data = change_log.changed_data or {}
        operation_type = change_log.operation_type
        
        if operation_type == "INSERT":
            # Add new data or initialize with initial data
            if 'initial_data' in changed_data:
                complete_data.extend(changed_data.get("initial_data", []))
            else:
                complete_data.extend(changed_data.get("added", []))
        
        elif operation_type == "UPDATE":
            # Update existing records
            for record in changed_data.get("modified", []):
                for existing_record in complete_data:
                    # Match by a unique field (e.g., "id" or "transaction_id")
                    if existing_record["id"] == record["id"]:
                        existing_record.update(record)
                        break
        
        elif operation_type == "DELETE":
            # Remove records
            complete_data = [
                record for record in complete_data
                if record not in changed_data.get("deleted", [])
            ]
    
    # Debugging: Print the final combined data
    print("Final Combined Data for Latest Version:", complete_data)

    return {
        "version_id": latest_version.version_id,
        "version_number": latest_version.version_number,
        "created_at": latest_version.created_at,
        "created_by": latest_version.created_by,
        "change_type": latest_version.change_type,
        "comment": latest_version.comment,
        "schema_definition": schema,
        "complete_data": complete_data
    }


@router.get("/{dataset_id}/versions/{version_number}", response_model=schemas.VersionInfo)
async def get_specific_version(
    dataset_id: UUID,
    version_number: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """Get a specific version of a dataset"""
    version = (
        db.query(models.Version)
        .filter(
            models.Version.dataset_id == dataset_id,
            models.Version.version_number == version_number
        )
        .first()
    )
    
    if not version:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Version {version_number} not found for dataset {dataset_id}"
        )
    
    # Fetch the schema for the version
    schema = (
        db.query(models.SchemaVersion)
        .filter(models.SchemaVersion.version_id == version.version_id)
        .all()
    )
    
    # Fetch all change logs up to and including the queried version
    change_logs = (
        db.query(models.ChangeLog)
        .join(models.Version)
        .filter(
            models.Version.dataset_id == dataset_id,
            models.Version.created_at <= version.created_at  # Only changes up to this version
        )
        .order_by(models.Version.created_at.asc())
        .all()
    )
    print("Change Logs:", change_logs)

    # Extract and debug the changed_data
    for change_log in change_logs:
        print("Operation Type:", change_log.operation_type)
        print("Changed Data:", change_log.changed_data)

    # Start with the initial data
    complete_data = []
    for change_log in change_logs:
        changed_data = change_log.changed_data or {}
        operation_type = change_log.operation_type
        
        if operation_type == "INSERT":
            # Add new data or initialize with initial data
            if 'initial_data' in changed_data:
                complete_data.extend(changed_data.get("initial_data", []))
            else:
                complete_data.extend(changed_data.get("added", []))
        
        elif operation_type == "UPDATE":
            # Update existing records
            for record in changed_data.get("modified", []):
                for existing_record in complete_data:
                    # Match by a unique field (e.g., "id" or "transaction_id")
                    if existing_record["id"] == record["id"]:
                        existing_record.update(record)
                        break
        
        elif operation_type == "DELETE":
            # Remove records
            complete_data = [
                record for record in complete_data
                if record not in changed_data.get("deleted", [])
            ]
    
    # Debugging: Print the final combined data
    print("Final Combined Data:", complete_data)

    return {
        "version_id": version.version_id,
        "version_number": version.version_number,
        "created_at": version.created_at,
        "created_by": version.created_by,
        "change_type": version.change_type,
        "comment": version.comment,
        "schema_definition": schema,
        "complete_data": complete_data
    }

     

@router.post("/{dataset_id}/versions", response_model=schemas.VersionResponse)
async def create_new_version(
    dataset_id: UUID,
    version: schemas.VersionCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """Create a new version of a dataset"""
    # Verify dataset exists
    dataset = db.query(models.Dataset).filter(models.Dataset.dataset_id == dataset_id).first()
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset with id {dataset_id} not found"
        )
    
    # Get latest version number
    latest_version = (
        db.query(models.Version)
        .filter(models.Version.dataset_id == dataset_id)
        .order_by(models.Version.created_at.desc())
        .first()
    )
    
    new_version_number = f"{float(latest_version.version_number) + 0.1:.1f}"
    
    # Create new version
    new_version = models.Version(
        dataset_id=dataset_id,
        version_number=new_version_number,
        created_by=current_user.id,
        change_type=version.change_type,
        comment=version.comment
    )
    db.add(new_version)
    db.flush()
    
    # Handle schema changes if any
    if version.schema_changes:
        for schema_change in version.schema_changes:
            schema_version = models.SchemaVersion(
                version_id=new_version.version_id,
                **schema_change.dict()
            )
            db.add(schema_version)
    else:
        # Copy schema from previous version
        previous_schema = (
            db.query(models.SchemaVersion)
            .filter(models.SchemaVersion.version_id == latest_version.version_id)
            .all()
        )
        for schema_item in previous_schema:
            new_schema = models.SchemaVersion(
                version_id=new_version.version_id,
                column_name=schema_item.column_name,
                data_type=schema_item.data_type,
                is_nullable=schema_item.is_nullable,
                description=schema_item.description
            )
            db.add(new_schema)
    
    # Record data changes
    change_log = models.ChangeLog(
        version_id=new_version.version_id,
        changed_by=current_user.id,
        operation_type=version.change_type,
        changed_data={"changes": version.data}
    )
    db.add(change_log)
    
    db.commit()
    db.refresh(new_version)
    return new_version



@router.get("/{dataset_id}/versions", response_model=schemas.VersionList)
async def list_versions(
    dataset_id: UUID,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """List all versions of a dataset"""
    versions = (
        db.query(models.Version)
        .filter(models.Version.dataset_id == dataset_id)
        .order_by(models.Version.created_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )
    
    # Get total count
    total_count = (
        db.query(models.Version)
        .filter(models.Version.dataset_id == dataset_id)
        .count()
    )
    
   # Get schema for each version
    version_list = []
    
    for version in versions:
        # Fetch schema for the version
        schema_versions = (
            db.query(models.SchemaVersion)
            .filter(models.SchemaVersion.version_id == version.version_id)
            .all()
        )
        
        # Convert schema to Pydantic format
        schema_definition = [
            schemas.SchemaDefinition(
                column_name=schema_version.column_name,
                data_type=schema_version.data_type,
                is_nullable=schema_version.is_nullable,
                description=schema_version.description
            )
            for schema_version in schema_versions
        ]
        
        # Create VersionListInfo object
        version_info = schemas.VersionListInfo(
            version_id=version.version_id,
            version_number=version.version_number,
            created_at=version.created_at,
            created_by=version.created_by,
            change_type=version.change_type,
            comment=version.comment,
            schema_definition=schema_definition
        )
        version_list.append(version_info)
    
    return schemas.VersionList(versions=version_list, total_count=total_count)