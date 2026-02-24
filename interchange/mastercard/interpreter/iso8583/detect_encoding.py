from interchange.persistence.database import Database

def obtain_encoding(
        db: Database,
        client_id: str,
        file_id: str
) -> str|None:
    df_file_control = db.read_sql(
        f"""
        SELECT
        file_type 
        FROM file_control
        WHERE upper(client_id) = upper(?) 
        AND upper(file_id) = upper(?)
        """,
        params=(client_id, file_id,),
    )

    file_type = str(df_file_control["file_type"].iloc[0]).strip().upper()

    print(f"file_type: {file_type}")
    if file_type == "IN":
        col = "file_mc_encoding_in"
    elif file_type == "OUT":
        col = "file_mc_encoding_out"
    else:
        return None

    df_client = db.read_sql(
        f"""
        SELECT {col} 
        FROM client 
        WHERE upper(client_id) = upper(?)
        """,
        params=(client_id,),
    )

    file_mc_encoding = str(df_client[col].iloc[0]).strip().upper()

    if file_mc_encoding in ("LATIN-1", "LATIN1", "ISO-8859-1", "ASCII"):
        return "Latin-1"
    elif file_mc_encoding in ("CP500", "EBCDIC", "EBDIC_DIGITS"):
        return "cp500"
    else:
        return None

