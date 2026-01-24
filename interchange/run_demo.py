# file: pipelines_runner.py
from concurrent.futures import ProcessPoolExecutor, as_completed
from interchange.persistence.file import FileStorage
from interchange.visa import transform, extract, clean, calculate, interchange, store
from interchange.mastercard import interpreter, mc_transform
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
        interpreter.interpretate_msg, # Function
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



if __name__ == "__main__":
    client_id = "BRDRO"
    file_id = "ba4a9711221a6b137c56ceb064f54a01"

    # client_id = "SBSA"
    # file_id = "85e91f44241d19d8bf23ce97d2bf49c9"

    # client_id = "BTRLRO"
    # file_id = "a3711894ebf22d0583df63cc5b5232dc" # incoming
    # file_id = "3bbe11a245223ecb2ebfb46b6d2c9f36" # incoming
    # file_id = "927e539ab0e66cbcf48cd6043cac1d47" # outgoing (block)
    # file_id = "28ef73ae78c526c130fccb618a581359" # outgoing (no block)
    # file_id = "cda240036fbee87e93277789a703b8e5" # outgoing (no block)

    #pipeline_visa_baseii(client_id, file_id)
    #pipeline_visa_sms(client_id, file_id)
    #pipeline_visa_vss(client_id, file_id)
    #pipeline_mc_interpreter(client_id,file_id)
    pipeline_mc_1240(client_id=client_id, file_id=file_id)


    #print("\n--- Tiempos de ejecución por función ---")
    for func_name, t in times.items():
        print(f"{func_name}: {t:.2f} s")

