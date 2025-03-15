# app/utils/dataset_utils.py
from typing import Dict, List, Any
import json
from datetime import datetime

def calculate_data_diff(old_data: List[Dict[str, Any]], new_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate differences between two versions of data"""
    diff = {
        "added": [],
        "modified": [],
        "deleted": []
    }
    
    old_keys = {json.dumps(item, sort_keys=True) for item in old_data}
    new_keys = {json.dumps(item, sort_keys=True) for item in new_data}
    
    # Find added and deleted items
    diff["added"] = [json.loads(item) for item in new_keys - old_keys]
    diff["deleted"] = [json.loads(item) for item in old_keys - new_keys]
    
    # Find modified items
    common_keys = old_keys & new_keys
    old_dict = {json.dumps(item, sort_keys=True): item for item in old_data}
    new_dict = {json.dumps(item, sort_keys=True): item for item in new_data}
    
    return diff

def validate_schema_compatibility(
    old_schema: List[Dict[str, Any]],
    new_schema: List[Dict[str, Any]]
) -> List[str]:
    """Validate if schema changes are backwards compatible"""
    warnings = []
    
    old_columns = {col["column_name"]: col for col in old_schema}
    new_columns = {col["column_name"]: col for col in new_schema}
    
    # Check for removed columns
    for col_name in old_columns:
        if col_name not in new_columns:
            warnings.append(f"Column '{col_name}' has been removed")
    
    # Check for modified columns
    for col_name, new_col in new_columns.items():
        if col_name in old_columns:
            old_col = old_columns[col_name]
            if new_col["data_type"] != old_col["data_type"]:
                warnings.append(
                    f"Data type changed for column '{col_name}': "
                    f"{old_col['data_type']} -> {new_col['data_type']}"
                )
            if old_col["is_nullable"] and not new_col["is_nullable"]:
                warnings.append(
                    f"Column '{col_name}' changed from nullable to non-nullable"
                )
    
    return warnings

def generate_version_metadata(
    dataset_id: str,
    version_number: str,
    change_type: str,
    changes: Dict[str, Any]
) -> Dict[str, Any]:
    """Generate metadata for a new version"""
    return {
        "dataset_id": dataset_id,
        "version_number": version_number,
        "change_type": change_type,
        "timestamp": datetime.utcnow().isoformat(),
        "changes_summary": {
            "total_changes": len(changes.get("changes", [])),
            "change_types": {
                "added": len(changes.get("added", [])),
                "modified": len(changes.get("modified", [])),
                "deleted": len(changes.get("deleted", []))
            }
        }
    }