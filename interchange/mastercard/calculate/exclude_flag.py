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



def df_exclude_flag(
    df: pd.DataFrame,    
    filepath: str,
    lookup_691: dict
) -> pd.DataFrame:
    
    file_identif = extract_file_identification(filepath)

    df["exclude_flag"] = 0

    if file_identif in lookup_691:
        valid_x = lookup_691[file_identif]
        mask = df["message_number_de_71"].isin(valid_x)

        # sanity check opcional
        #if mask.sum() > 1:
        #    raise ValueError(
        #        f"Más de un match en {filepath} para file_identif={file_identif}"
        #    )

        df.loc[mask, "exclude_flag"] = 1

    return df