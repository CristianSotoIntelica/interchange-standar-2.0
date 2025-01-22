from interchange.persistence.file import FileStorage
from interchange.visa import transform, extract, clean, calculate, interchange, store


layer = FileStorage.Layer


if __name__ == "__main__":
    # Step 1: Define parameters for demo execution:
    client_id = "DEMO"
    file_id = "CDA26F0BEB4349D03346A721DDCF0DC7"
    # Step 1: Transform BASE II file into parquet of separate transactions:
    transform.transform_baseii_drafts(
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
    # Step 3: Clean data fields and transform data types where required:
    clean.clean_baseii_fields(
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
