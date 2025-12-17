"""
File detection and ingestion module.

This module scans the LANDING directory for new files, identifies their type
using configured regex patterns, and registers them in the file_control table.
"""

import re
import uuid
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage


log = Logger(__name__)
db = Database()
fs = FileStorage()


# =============================================================================
# FILE DETECTION FUNCTIONS
# =============================================================================


def scan_landing_files(
    landing_layer: FileStorage.Layer,
    client_id: str,
) -> list[dict]:
    """
    Scan landing directory for files belonging to a client.
    
    Args:
        landing_layer: Landing storage layer
        client_id: Client identifier
        
    Returns:
        List of dictionaries with file metadata:
        - file_path: Full path to file
        - file_name: File name
        - file_size: File size in bytes
        - file_date: File modification date
    """
    log.logger.info(f"Scanning landing directory for client {client_id}")
    
    # Get landing directory path for client
    landing_path = fs._get_path(landing_layer, client_id, "")
    
    if not landing_path.exists():
        log.logger.warning(f"Landing directory does not exist: {landing_path}")
        return []
    
    files = []
    for file_path in landing_path.glob("*"):
        if file_path.is_file():
            file_stat = file_path.stat()
            files.append({
                "file_path": str(file_path),
                "file_name": file_path.name,
                "file_size": file_stat.st_size,
                "file_date": datetime.fromtimestamp(file_stat.st_mtime).date(),
            })
    
    log.logger.info(f"Found {len(files)} file(s) in landing directory")
    return files


def get_file_format_configs(client_id: str) -> pd.DataFrame:
    """
    Get file format configurations for a client from database.
    
    Retrieves regex patterns and metadata used to identify file types.
    Prioritizes client-specific configs (customer_code) over generic (ALL).
    
    Args:
        client_id: Client identifier
        
    Returns:
        DataFrame with columns:
        - brand
        - customer_code
        - file_type
        - file_format (regex pattern)
        - priority (0 for ALL, 1 for specific customer_code)
    """
    log.logger.info(f"Loading file format configurations for client {client_id}")
    
    # Query file_format table (or whatever your table name is)
    configs = db.read_records(
        table_name="file_format",  # Adjust table name if different
        fields=["brand", "customer_code", "file_type", "file_format"],
        where={"customer_code": ["ALL", client_id]},  # Get ALL and client-specific
    )
    
    if configs.empty:
        log.logger.warning(f"No file format configurations found for client {client_id}")
        return configs
    
    # Add priority: specific customer_code (1) > ALL (0)
    configs["priority"] = configs["customer_code"].apply(
        lambda x: 1 if x != "ALL" else 0
    )
    
    # Sort by priority descending (try specific first, then ALL)
    configs = configs.sort_values("priority", ascending=False)
    
    log.logger.info(f"Loaded {len(configs)} file format configuration(s)")
    return configs


def identify_file_type(
    file_name: str,
    configs: pd.DataFrame,
) -> Optional[dict]:
    """
    Identify file type by matching file name against regex patterns.
    
    Args:
        file_name: Name of the file
        configs: DataFrame with file format configurations (from get_file_format_configs)
        
    Returns:
        Dictionary with identified metadata:
        - brand: VISA, MASTERCARD, etc.
        - customer_code: Client-specific code or ALL
        - file_type: VI Outgoing, VI Incoming, MC Outgoing, etc.
        - file_format: Matched regex pattern
        
        Returns None if no match found.
    """
    log.logger.debug(f"Identifying file type for: {file_name}")
    
    for _, config in configs.iterrows():
        pattern = config["file_format"]
        
        try:
            # Match regex pattern against file name
            if re.search(pattern, file_name):
                log.logger.info(
                    f"File matched: {file_name} -> "
                    f"{config['brand']} | {config['file_type']} | "
                    f"pattern: {pattern}"
                )
                return {
                    "brand": config["brand"],
                    "customer_code": config["customer_code"],
                    "file_type": config["file_type"],
                    "file_format": pattern,
                }
        except re.error as e:
            log.logger.error(f"Invalid regex pattern '{pattern}': {e}")
            continue
    
    log.logger.warning(f"No matching configuration found for file: {file_name}")
    return None


def extract_file_date(file_name: str) -> Optional[date]:
    """
    Extract file processing date from file name.
    
    Common patterns:
    - YYYYMMDD (8 digits)
    - YYMMDD (6 digits)
    
    Args:
        file_name: Name of the file
        
    Returns:
        Extracted date or None if not found
    """
    # Try YYYYMMDD pattern
    match = re.search(r"(\d{8})", file_name)
    if match:
        try:
            date_str = match.group(1)
            return datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            pass
    
    # Try YYMMDD pattern
    match = re.search(r"(\d{6})", file_name)
    if match:
        try:
            date_str = match.group(1)
            return datetime.strptime(date_str, "%y%m%d").date()
        except ValueError:
            pass
    
    return None


def register_file_in_control(
    client_id: str,
    file_name: str,
    file_type_info: dict,
    file_date: Optional[date] = None,
) -> str:
    """
    Register a file in the file_control table.
    
    Args:
        client_id: Client identifier
        file_name: Name of the file
        file_type_info: Dictionary from identify_file_type()
        file_date: File processing date (if None, uses current date)
        
    Returns:
        file_id: Generated UUID for the file
    """
    file_id = str(uuid.uuid4()).replace("-", "").upper()
    
    if file_date is None:
        file_date = date.today()
    
    # Determine file_type code (IN/OUT)
    if "Incoming" in file_type_info["file_type"]:
        file_type_code = "IN"
    elif "Outgoing" in file_type_info["file_type"]:
        file_type_code = "OUT"
    else:
        file_type_code = "UNKNOWN"
    
    # Prepare record for insertion
    file_record = {
        "file_id": file_id,
        "client_id": client_id,
        "brand_id": file_type_info["brand"],
        "file_type": file_type_code,
        "file_format": file_type_info["file_format"],
        "file_name": file_name,
        "file_processing_date": file_date,
        "file_status": "PENDING",
        "created_at": datetime.now(),
    }
    
    log.logger.info(f"Registering file in file_control: {file_id} | {file_name}")
    
    # Insert into database
    db.write_records(
        table_name="file_control",
        data=pd.DataFrame([file_record]),
    )
    
    log.logger.info(f"File registered successfully: {file_id}")
    return file_id


def is_file_already_registered(
    client_id: str,
    file_name: str,
) -> bool:
    """
    Check if a file has already been registered in file_control.
    
    Args:
        client_id: Client identifier
        file_name: Name of the file
        
    Returns:
        True if file already exists in file_control, False otherwise
    """
    existing = db.read_records(
        table_name="file_control",
        fields=["file_id"],
        where={"client_id": client_id, "file_name": file_name},
    )
    
    return not existing.empty


# =============================================================================
# MAIN DETECTION WORKFLOW
# =============================================================================


def detect_and_register_files(
    landing_layer: FileStorage.Layer,
    client_id: str,
    skip_existing: bool = True,
) -> list[dict]:
    """
    Main workflow: Scan landing directory, identify files, and register in file_control.
    
    This is the primary function to use for file ingestion.
    
    Args:
        landing_layer: Landing storage layer
        client_id: Client identifier
        skip_existing: If True, skip files already registered in file_control
        
    Returns:
        List of dictionaries with registered file information:
        - file_id: Generated UUID
        - file_name: Name of the file
        - brand: VISA, MASTERCARD, etc.
        - file_type: IN/OUT
        - status: "registered" or "skipped" or "failed"
        - reason: Error message if failed
    """
    log.logger.info("=" * 80)
    log.logger.info(f"FILE DETECTION AND REGISTRATION - Client: {client_id}")
    log.logger.info("=" * 80)
    
    results = []
    
    # Step 1: Scan landing directory
    files = scan_landing_files(landing_layer, client_id)
    
    if not files:
        log.logger.info("No files found in landing directory")
        return results
    
    # Step 2: Load file format configurations
    configs = get_file_format_configs(client_id)
    
    if configs.empty:
        log.logger.error("No file format configurations found. Cannot identify files.")
        return results
    
    # Step 3: Process each file
    for file_info in files:
        file_name = file_info["file_name"]
        
        log.logger.info("-" * 80)
        log.logger.info(f"Processing file: {file_name}")
        
        try:
            # Check if already registered
            if skip_existing and is_file_already_registered(client_id, file_name):
                log.logger.info(f"File already registered, skipping: {file_name}")
                results.append({
                    "file_id": None,
                    "file_name": file_name,
                    "brand": None,
                    "file_type": None,
                    "status": "skipped",
                    "reason": "Already registered",
                })
                continue
            
            # Identify file type
            file_type_info = identify_file_type(file_name, configs)
            
            if file_type_info is None:
                log.logger.warning(f"Could not identify file type: {file_name}")
                results.append({
                    "file_id": None,
                    "file_name": file_name,
                    "brand": None,
                    "file_type": None,
                    "status": "failed",
                    "reason": "No matching configuration",
                })
                continue
            
            # Extract file date from name (if possible)
            file_date = extract_file_date(file_name)
            if file_date is None:
                file_date = file_info["file_date"]
                log.logger.info(
                    f"Could not extract date from filename, using file modification date: {file_date}"
                )
            
            # Register in file_control
            file_id = register_file_in_control(
                client_id,
                file_name,
                file_type_info,
                file_date,
            )
            
            results.append({
                "file_id": file_id,
                "file_name": file_name,
                "brand": file_type_info["brand"],
                "file_type": file_type_info["file_type"],
                "status": "registered",
                "reason": None,
            })
            
        except Exception as e:
            log.logger.error(f"Error processing file {file_name}: {str(e)}")
            results.append({
                "file_id": None,
                "file_name": file_name,
                "brand": None,
                "file_type": None,
                "status": "failed",
                "reason": str(e),
            })
    
    # Summary
    log.logger.info("=" * 80)
    log.logger.info("DETECTION SUMMARY")
    log.logger.info("=" * 80)
    
    registered = sum(1 for r in results if r["status"] == "registered")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed = sum(1 for r in results if r["status"] == "failed")
    
    log.logger.info(f"Total files:      {len(results)}")
    log.logger.info(f"Registered:       {registered}")
    log.logger.info(f"Skipped:          {skipped}")
    log.logger.info(f"Failed:           {failed}")
    log.logger.info("=" * 80)
    
    return results


def get_pending_files(client_id: Optional[str] = None) -> pd.DataFrame:
    """
    Get list of files in PENDING status from file_control.
    
    Args:
        client_id: Optional client identifier. If None, returns all pending files.
        
    Returns:
        DataFrame with pending files
    """
    where = {"file_status": "PENDING"}
    if client_id:
        where["client_id"] = client_id
    
    pending = db.read_records(
        table_name="file_control",
        fields=[
            "file_id",
            "client_id",
            "brand_id",
            "file_type",
            "file_name",
            "file_processing_date",
        ],
        where=where,
    )
    
    return pending


def update_file_status(
    file_id: str,
    new_status: str,
    error_message: Optional[str] = None,
) -> None:
    """
    Update file status in file_control table.
    
    Args:
        file_id: File identifier
        new_status: New status (e.g., "PROCESSING", "COMPLETED", "FAILED")
        error_message: Optional error message if status is FAILED
    """
    update_data = {
        "file_status": new_status,
        "updated_at": datetime.now(),
    }
    
    if error_message:
        update_data["error_message"] = error_message
    
    db.update_records(
        table_name="file_control",
        data=pd.DataFrame([update_data]),
        where={"file_id": file_id},
    )
    
    log.logger.info(f"Updated file status: {file_id} -> {new_status}")