from interchange.persistence.file import FileStorage
from interchange.visa import transform, extract, clean, calculate, interchange, store

#Csotopariona


layer = FileStorage.Layer


if __name__ == "__main__":
    # Step 1: Define parameters for demo execution:
    client_id = "EBGR"
    file_id = "46b51a10d62a0f73b3fda8b5cd749159"
    # Step 1: Transform BASE II file into parquet of separate transactions:
    transform.transform_baseii_drafts(
        origin_layer=layer.LANDING,
        target_layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
    )

    # Step 1: Transform VSS records into parquet of separate transactions:
    print("\n" + "="*80)
    print("STEP 1: TRANSFORM VSS - RAW (100)")
    print("="*80)
    transform.transform_vss_records(
        origin_layer=layer.LANDING,
        target_layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
    )

    # Step 2: Extract individual data fields from each transaction:
    extract.extract_baseii_fields(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
    )

    print("\n" + "="*80)
    print("STEP 2: EXTRACT VSS - EXT (200)")
    print("="*80)
    extract.extract_vss_fields(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
    )

    # Step 3: Clean data fields and transform data types where required:
    clean.clean_baseii_fields(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
    )

    print("\n" + "="*80)
    print("STEP 3: CLEAN VSS - CLN (300)")
    print("="*80)
    clean.clean_vss_fields(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
    )

    # Step 4: Calculate additional fields using transaction and master data:
    calculate.calculate_baseii_fields(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
    )

    print("\n" + "="*80)
    print("STEP 4: Calculate VSS - CAL (400)")
    print("="*80)
    calculate.calculate_vss_fields(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
    )

    # Step 5: Calculate interchange fee fields and amount:
    interchange.calculate_baseii_interchange(
        origin_layer=layer.STAGING,
        target_layer=layer.STAGING,
        client_id=client_id,
        file_id=file_id,
    )
    # Step 6: Store full file data into the operational layer.
    store.store_baseii_file(
        origin_layer=layer.STAGING,
        target_layer=layer.OPERATIONAL,
        client_id=client_id,
        file_id=file_id,
    )

    print("\n" + "="*80)
    print("STEP 6: STORE VSS - OPERATIONAL")
    print("="*80)
    store.store_vss_file(
        origin_layer=layer.STAGING,
        target_layer=layer.OPERATIONAL,
        client_id=client_id,
        file_id=file_id,
    )