# file: pipelines_runner.py
from concurrent.futures import ProcessPoolExecutor, as_completed
from interchange.persistence.file import FileStorage
from interchange.visa import transform, extract, clean, calculate, interchange, store
from interchange.mastercard import (
    mc_interpreter, mc_transform, mc_extract, mc_clean, mc_calculate, mc_interchange
)
from pathlib import Path
import gc
import time  # <-- añadimos time

layer = FileStorage.Layer

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Diccionario global para almacenar tiempos

times = {}

def timed(func, *args, **kwargs):
    start = time.perf_counter()
    result = func(*args, **kwargs)
    end = time.perf_counter()
    times[func.__name__] = end - start
    return result

def pipeline_visa_sms(client_id, file_id):
    timed(
        transform.transform_sms_messages,
        layer.LANDING,
        layer.STAGING,
        client_id,
        file_id,
    )
    gc.collect()

    timed(extract.extract_sms_fields, layer.STAGING, layer.STAGING, client_id, file_id)
    gc.collect()

    timed(clean.clean_sms_fields, layer.STAGING, layer.STAGING, client_id, file_id)
    gc.collect()

    timed(
        calculate.calculate_sms_fields, layer.STAGING, layer.STAGING, client_id, file_id
    )
    gc.collect()

    timed(
        interchange.calculate_sms_interchange,
        layer.STAGING,
        layer.STAGING,
        client_id,
        file_id,
    )
    gc.collect()

    timed(store.store_sms_file, layer.STAGING, layer.OPERATIONAL, client_id, file_id)
    gc.collect()

def pipeline_visa_baseii(client_id, file_id):
    timed(
        transform.transform_baseii_drafts,
        layer.LANDING,
        layer.STAGING,
        client_id,
        file_id,
    )
    gc.collect()

    timed(
        extract.extract_baseii_fields, layer.STAGING, layer.STAGING, client_id, file_id
    )
    gc.collect()

    timed(clean.clean_baseii_fields, layer.STAGING, layer.STAGING, client_id, file_id)
    gc.collect()

    timed(
        calculate.calculate_baseii_fields,
        layer.STAGING,
        layer.STAGING,
        client_id,
        file_id,
    )
    gc.collect()

    timed(
        interchange.calculate_baseii_interchange,
        layer.STAGING,
        layer.STAGING,
        client_id,
        file_id,
    )
    gc.collect()

    timed(store.store_baseii_file, layer.STAGING, layer.OPERATIONAL, client_id, file_id)
    gc.collect()

def pipeline_visa_vss(client_id, file_id):
    timed(
        transform.transform_vss_records,
        layer.LANDING,
        layer.STAGING,
        client_id,
        file_id,
    )
    gc.collect()

    timed(extract.extract_vss_fields, layer.STAGING, layer.STAGING, client_id, file_id)
    gc.collect()

    timed(clean.clean_vss_fields, layer.STAGING, layer.STAGING, client_id, file_id)
    gc.collect()

    timed(
        calculate.calculate_vss_fields, layer.STAGING, layer.STAGING, client_id, file_id
    )
    gc.collect()

    timed(store.store_vss_file, layer.STAGING, layer.OPERATIONAL, client_id, file_id)
    gc.collect()

def pipeline_mc_interpreter(client_id: str, file_id: str):

    timed(
        mc_interpreter.interpretate_msg, # Function
        layer.LANDING, # origin_layer 
        layer.STAGING, # target_layer
        client_id, # client_id (bank)
        file_id, # file_id (md5)
    )

def pipeline_mc_1240(client_id: str, file_id: str):

    timed(
        mc_transform.transform_ipm_1240, # Function
        layer.STAGING, # origin_layer
        layer.STAGING, # target_layer
        client_id, # client_id (bank)
        file_id # file_id (md5)
    )

    timed(
        mc_extract.extract_1240_fields, 
        layer.STAGING, # origin_target
        layer.STAGING, # target_target
        client_id, # client_id (bank)
        file_id, # file_id (md5)
    )

    timed(
        mc_clean.clean_1240_fields, 
        layer.STAGING, # origin_target
        layer.STAGING, # target_target
        client_id, # client_id (bank)
        file_id, # file_id (md5)
    )

    timed(
        mc_calculate.calculate_1240_fields, 
        layer.STAGING, # origin_target
        layer.STAGING, # target_target
        client_id, # client_id (bank)
        file_id, # file_id (md5)
    )
    
    timed(
        mc_interchange.interchange_1240_fields, 
        layer.STAGING, # origin_target
        layer.STAGING, # target_target
        client_id, # client_id (bank)
        file_id, # file_id (md5)
    )

def pipeline_mc_1442(client_id: str, file_id: str):

    timed(
        mc_transform.transform_ipm_1442, # Function
        layer.STAGING, # origin_layer
        layer.STAGING, # target_layer
        client_id, # client_id (bank)
        file_id # file_id (md5)
    )

    timed(
        mc_extract.extract_1442_fields, 
        layer.STAGING, # origin_target
        layer.STAGING, # target_target
        client_id, # client_id (bank)
        file_id, # file_id (md5)
    )

    timed(
        mc_clean.clean_1442_fields, 
        layer.STAGING, # origin_target
        layer.STAGING, # target_target
        client_id, # client_id (bank)
        file_id, # file_id (md5)
    )

def pipeline_mc_1644(client_id: str, file_id: str):

    timed(
        mc_transform.transform_ipm_1644, # Function
        layer.STAGING, # origin_layer
        layer.STAGING, # target_layer
        client_id, # client_id (bank)
        file_id # file_id (md5)
    )

    timed(
        mc_extract.extract_1644_fields,
        layer.STAGING, # origin_layer
        layer.STAGING, # target_layer
        client_id, # client_id (bank)
        file_id # file_id (md5)
    )

    timed(
        mc_clean.clean_1644_fields,
        layer.STAGING, 
        layer.STAGING,
        client_id,
        file_id 
    )

def pipeline_mc_1740(client_id: str, file_id: str):

    timed(
        mc_transform.transform_ipm_1740, # Function
        layer.STAGING, # origin_layer
        layer.STAGING, # target_layer
        client_id, # client_id (bank)
        file_id # file_id (md5)
    )

    timed(
        mc_extract.extract_1740_fields,
        layer.STAGING, # origin_layer
        layer.STAGING, # target_layer
        client_id, # client_id (bank)
        file_id # file_id (md5)
    )

    timed(
        mc_clean.clean_1740_fields, 
        layer.STAGING, # origin_target
        layer.STAGING, # target_target
        client_id, # client_id (bank)
        file_id, # file_id (md5)
    )

if __name__ == "__main__":
    # client_id = "BRDRO"
    # file_id = "e0cdccf3be383ecd2c8044b40c02be44"

    # client_id = "SBSA" # LISTO - VALIDADO
    # file_id = "85e91f44241d19d8bf23ce97d2bf49c9" # incoming | MasterCard_Inward_Settlement_to_SBSA_T112_20260113.TXT | 
    # file_id = "074b0b73807ff7833e900149225182d2" # incoming | MasterCard_Inward_Settlement_to_SBSA_T112_20260218.TXT |
    # file_id = "5055a175555561b9ebbfa174597768d4" # outgoing | MasterCard_Outward_Settlement_from_SBSA_R111_20260218.TXT |
    
    client_id = "BTRLRO" # LISTO - VALIDADO
    # file_id = "a3711894ebf22d0583df63cc5b5232dc" # Incoming | MCI.AR.T112.M.E0078853.D260107.T004452.A003 |
    # file_id = "3bbe11a245223ecb2ebfb46b6d2c9f36" # Incoming | MCI.AR.T112.M.E0078853.D260107.T034734.A004 | 
    # file_id = "927e539ab0e66cbcf48cd6043cac1d47" # outgoing | IPM_6007.O00063 | (block)
    # file_id = "28ef73ae78c526c130fccb618a581359" # outgoing |  | (no block) # PONERLE BLOCK
    file_id = "cda240036fbee87e93277789a703b8e5" # outgoing |  | (no block) # PONERLE BLOCK

    # client_id = "EURBGR" # LISTO - VALIDADO
    # file_id = "97dc629e881368e4f80dc732f0f07803" # Incoming | T112T0.2026-01-07-13-10-06.001 | 
    # file_id = "bb96c101dab92e33221f55a107819c52" # Incoming | T112T0.2026-01-07-13-10-29.001 |
    # file_id = "2add10e3aebde5ae94f03cfd51552f56" # Incoming | T112T0.2026-01-14-13-10-06.001 |
    # file_id = "846c3fbdfa8454ab0e902c947159acdd" # Incoming | T112T0.2026-01-14-13-12-09.001 |
    # file_id = "e5fdcc1435061b41e8965433870f5263" # Incoming | T112T0.2026-01-21-13-10-06.001 |
    # file_id = "2e83121625b788ef9c48370f792823df" # Incoming | T112T0.2026-01-21-13-11-33.001 |
    # file_id = "db7f1de4075536e2bae5d1d6a4f22c75" # Incoming | T112T0.2026-01-28-13-10-05.001 |
    # file_id = "5a3735ce3378ecc56bef99f046468d0e" # Incoming | T112T0.2026-01-28-13-11-36.001 |
    # file_id = "04e1c505846c589ba11009420bdfd7ff" # Incoming | T112T0.2026-01-28-13-12-47.001 | 
    # file_id = "83c3bf7e9a4a6b7aa7ac484e903e33c3" # Incoming | T112T0.2026-01-28-16-30-08.001 |
    # file_id = "02c14e0dc220c139e7a69b0abcad1443" # Incoming | T112T0.2026-02-12-13-10-06.001 |
    # file_id = "218175802e6785fe6e432a43328097be" # Incoming | T112T0.2026-02-13-13-10-05.001 |
    # file_id = "b7f58af0ab1ca70c077c9da1f189f976" # Incoming | T112T0.2026-02-14-13-10-05.001 |

    # pipeline_visa_baseii(client_id, file_id)
    # pipeline_visa_sms(client_id, file_id)
    # pipeline_visa_vss(client_id, file_id)
    
    pipeline_mc_interpreter(client_id,file_id)
    # pipeline_mc_1644(client_id=client_id, file_id=file_id)
    # pipeline_mc_1240(client_id=client_id, file_id=file_id)
    # pipeline_mc_1442(client_id=client_id, file_id=file_id)
    # pipeline_mc_1740(client_id=client_id, file_id=file_id)
    
    #print("\n--- Tiempos de ejecución por función ---")
    for func_name, t in times.items():
        print(f"{func_name}: {t:.2f} s")

