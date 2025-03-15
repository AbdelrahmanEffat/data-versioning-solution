# app/api/endpoints/enhanced_versions.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime
from ...core.database import get_db
from ...utils.enhanced_operations import EnhancedDataOperations, get_version_with_cache
from ...utils.oauth2 import get_current_user
from ...schemas import schemas

router = APIRouter(
    prefix="/datasets/enhanced",
    tags=['Enhanced Dataset Operations']
)

# Initialize enhanced operations
enhanced_ops = EnhancedDataOperations()

@router.post("/{dataset_id}/bulk", response_model=Dict[str, Any])
async def bulk_upload_data(
    dataset_id: str,
    data: List[Dict[str, Any]],
    batch_size: int = Query(1000, gt=0, le=5000),
    db: Session = Depends(get_db),
    current_user: schemas.UserOut = Depends(get_current_user)
):
    """Bulk upload data with batching support"""
    return await enhanced_ops.bulk_insert(db, dataset_id, data, batch_size)

@router.get("/{dataset_id}/versions/search", response_model=List[Dict[str, Any]])
async def search_dataset_versions(
    dataset_id: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    change_type: Optional[str] = None,
    schema_changes: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
    current_user: schemas.UserOut = Depends(get_current_user)
):
    """Search versions with advanced filters"""
    filters = {
        "date_range": {"start": start_date, "end": end_date} if start_date and end_date else None,
        "change_type": change_type,
        "schema_changes": schema_changes
    }
    filters = {k: v for k, v in filters.items() if v is not None}
    return await enhanced_ops.search_versions(db, dataset_id, filters)

@router.get("/{dataset_id}/versions/{version_number}/cached")
async def get_cached_version(
    dataset_id: str,
    version_number: str,
    db: Session = Depends(get_db),
    current_user: schemas.UserOut = Depends(get_current_user)
):
    """Get version data with caching support"""
    return await get_version_with_cache(enhanced_ops, db, dataset_id, version_number)