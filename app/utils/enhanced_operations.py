# app/utils/enhanced_operations.py
from typing import Dict, List, Any, Optional
import zlib
import json
import redis
from fastapi import HTTPException
from sqlalchemy.orm import Session
from datetime import timedelta
import pandas as pd
from ..models import models

class EnhancedDataOperations:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        """Initialize with Redis connection for caching"""
        self.redis_client = redis.from_url(redis_url)
        
    def compress_data(self, data: Dict[str, Any]) -> bytes:
        """Compress data using zlib"""
        return zlib.compress(json.dumps(data).encode())

    def decompress_data(self, compressed_data: bytes) -> Dict[str, Any]:
        """Decompress zlib compressed data"""
        return json.loads(zlib.decompress(compressed_data).decode())

    def cache_version(self, version_id: str, data: Dict[str, Any], expire_time: int = 3600):
        """Cache version data with expiration"""
        compressed_data = self.compress_data(data)
        self.redis_client.setex(f"version:{version_id}", expire_time, compressed_data)

    def get_cached_version(self, version_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached version data"""
        cached_data = self.redis_client.get(f"version:{version_id}")
        if cached_data:
            return self.decompress_data(cached_data)
        return None

    async def bulk_insert(self, db: Session, dataset_id: str, data: List[Dict[str, Any]], 
                         batch_size: int = 1000) -> Dict[str, Any]:
        """Efficiently handle bulk data insertion"""
        try:
            # Convert to DataFrame for efficient processing
            df = pd.DataFrame(data)
            
            # Process in batches
            total_records = 0
            for i in range(0, len(df), batch_size):
                batch = df[i:i + batch_size]
                batch_data = batch.to_dict('records')
                
                # Create change log entry for batch
                change_log = models.ChangeLog(
                    version_id=dataset_id,
                    operation_type="INSERT",
                    changed_data={"batch_data": batch_data}
                )
                db.add(change_log)
                total_records += len(batch)
                
                # Commit each batch
                if i % (batch_size * 5) == 0:
                    db.commit()
            
            db.commit()
            return {"status": "success", "records_processed": total_records}
            
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Bulk insert failed: {str(e)}")

    async def search_versions(self, db: Session, dataset_id: str, 
                            filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Advanced search functionality for versions"""
        query = db.query(models.Version).filter(
            models.Version.dataset_id == dataset_id
        )
        
        # Apply filters
        if 'date_range' in filters:
            query = query.filter(
                models.Version.created_at.between(
                    filters['date_range']['start'],
                    filters['date_range']['end']
                )
            )
            
        if 'change_type' in filters:
            query = query.filter(
                models.Version.change_type == filters['change_type']
            )
            
        if 'schema_changes' in filters:
            query = query.join(models.SchemaVersion).filter(
                models.SchemaVersion.column_name.in_(filters['schema_changes'])
            )
        
        results = query.all()
        return [self._format_version_result(version) for version in results]

    def _format_version_result(self, version: models.Version) -> Dict[str, Any]:
        """Format version results with relevant metadata"""
        return {
            "version_id": str(version.version_id),
            "version_number": version.version_number,
            "created_at": version.created_at.isoformat(),
            "change_type": version.change_type,
            "schema_versions": [
                {
                    "column_name": sv.column_name,
                    "data_type": sv.data_type,
                    "is_nullable": sv.is_nullable
                }
                for sv in version.schema_versions
            ]
        }

# Usage example in routes
async def get_version_with_cache(
    enhanced_ops: EnhancedDataOperations,
    db: Session,
    dataset_id: str,
    version_number: str
) -> Dict[str, Any]:
    """Get version data with caching support"""
    cache_key = f"{dataset_id}:{version_number}"
    
    # Try cache first
    cached_data = enhanced_ops.get_cached_version(cache_key)
    if cached_data:
        return cached_data
        
    # If not in cache, get from database
    version = db.query(models.Version).filter(
        models.Version.dataset_id == dataset_id,
        models.Version.version_number == version_number
    ).first()
    
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")
        
    # Format and cache the result
    result = {
        "version_info": enhanced_ops._format_version_result(version),
        "data": [change.changed_data for change in version.changes]
    }
    
    enhanced_ops.cache_version(cache_key, result)
    return result