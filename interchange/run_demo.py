# file: pipelines_runner.py
from concurrent.futures import ProcessPoolExecutor, as_completed
from interchange.persistence.file import FileStorage
from interchange.visa import transform, extract, clean, calculate, interchange, store
from interchange.mastercard import interpreter
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
    test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "DEMO" / "MCI.AR.R111.C.E0085850.D260108.T011121.A001"

    timed(
        interpreter.interpretate_msg,
        layer.LANDING,
        layer.LANDING,
        client_id,
        file_id,
        test_path=str(test_path),   #keyword para que no se vaya a origin_subdir
    )

if __name__ == "__main__":
    client_id = "EBGR"
    file_id = "CD976168BF6706C7FE71916C1A38DF2D"

    #pipeline_visa_baseii(client_id, file_id)
    #pipeline_visa_sms(client_id, file_id)
    #pipeline_visa_vss(client_id, file_id)
    pipeline_mc_interpreter(client_id,file_id)
    #print("\n--- Tiempos de ejecución por función ---")
    for func_name, t in times.items():
        print(f"{func_name}: {t:.2f} s")

