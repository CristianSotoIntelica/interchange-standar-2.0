from interchange.persistence.file import FileStorage

from interchange.mastercard.clean.fields_dtype_def import cast_df_from_params_def, build_arrow_schema_from_params
from interchange.mastercard.clean.metadata import load_mc_field_dtype_definitions, extend_field_defs_with_base_cols

fs = FileStorage()
import pandas as pd

layer = FileStorage.Layer


def extract_file_identification(filepath: str) -> str:
    filename = filepath.rsplit("\\", 1)[-1]
    return filename.split("_")[1]


def build_lookup_691(
    origin_layer,
    client_id: str,
    file_id: str,
    origin_sub_dir: str,
    excluded_fc: set[str] = {"691"}
) -> dict:
    """
    Construye un lookup:
        file_identification -> set(values de source_message_number_id_pds_138)
    a partir de parquets 691.
    """

    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir
    )

    fc_691_files = []
    for f in list_filepaths:
        fc = f.rsplit(".", 1)[0].rsplit("_", 1)[-1]
        if fc in excluded_fc:
            fc_691_files.append(f)

    lookup_691 = {}

    for f in fc_691_files:
        file_identif = extract_file_identification(f)
        df_691 = pd.read_parquet(f)

        values_x = set(
            df_691["source_message_number_id_pds_138"]
            .dropna()
            .unique()
        )

        lookup_691[file_identif] = values_x

    return lookup_691



def create_df_exclude_flag(
    df: pd.DataFrame,    
    filepath: str,
    lookup_691: dict
) -> pd.DataFrame:
    
    file_identif = extract_file_identification(filepath)

    df["exclude_flag"] = 0

    if file_identif in lookup_691:
        valid_x = lookup_691[file_identif]
        mask = df["message_number_de_71"].isin(valid_x)

        df.loc[mask, "exclude_flag"] = 1
    
    df_excluded = df[df["exclude_flag"] == 1]

    cols_to_keep = ["file_idn", "file_dt", "type_mti", "ref_id", "exclude_flag"]
    existing_cols = [c for c in cols_to_keep if c in df_excluded.columns]

    return df_excluded.loc[:, existing_cols]

def add_exclude_flag(
        df: pd.DataFrame,
        df_exclude: pd.DataFrame,
) -> pd.DataFrame:
    df["exclude_flag"] = 0 
    keys = ["file_idn", "file_dt", "type_mti", "ref_id"]

    df_final = df.merge(
        df_exclude, 
        on=keys,
        how="left", 
        suffixes=("", "_ex")
    )

    df_final["exclude_flag"] = df_final["exclude_flag_ex"].fillna(df_final["exclude_flag"]).astype("int64")
    df_final = df_final.drop(columns=["exclude_flag_ex"])

    return df_final