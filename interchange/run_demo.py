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
    # Standard Bank
    #test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "DEMO" / "MasterCard_Inward_Settlement_to_SBSA_T112_20260113.TXT"
    ####################################################################################
    # BRDRO
    # BRDRO INCOMING
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "BRDRO" / "MI260107.001"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "BRDRO" / "MI260107.004"
    
    # # BRDRO OUTGOING
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "BRDRO" / "MO260107.001"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "BRDRO" / "MO260107.003"
    # ####################################################################################
    # # ERSTHU
    # # ERSTHU INCOMING
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "ERSTHU" / "MCI.AR.T112.C.E0085850.D260107.T004239.A003"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "ERSTHU" / "MCI.AR.T112.C.E0085850.D260107.T082746.A006"
    
    # # ERSTHU OUTGOING
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "ERSTHU" / "MCI.AR.R111.C.E0085850.D260107.T012958.A002"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "ERSTHU" / "MCI.AR.R111.C.E0085850.D260107.T012940.A001"
    # ####################################################################################
    # # BTRLRO
    # # BTRLRO INCOMING
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "BTRLRO" / "MCI.AR.T112.M.E0078853.D260107.T004452.A003"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "BTRLRO" / "MCI.AR.T112.M.E0078853.D260107.T034734.A004"

    # # BTRLRO OUTGOING
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "BTRLRO" / "OMC_20260107_002938_0001"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "BTRLRO" / "OMC_20260107_022705_0001"

    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "BTRLRO" / "IPM_6007.O00063"
    # ####################################################################################
    # # EURBGR
    # # EURBGR INCOMING
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "EURBGR" / "T112T0.2026-01-07-13-10-29.001"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "EURBGR" / "T112T0.2026-01-07-13-10-06.001"

    # # EURBGR OUTGOING
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "EURBGR" / "TOCEURO-2026-01-06-08-00-40-20260107"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "EURBGR" / "TOCEURO-2026-01-07-08-00-12-20260107"

    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "EURBGR" / "TOCEURO4-2026-01-06-08-00-41-20260107"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "EURBGR" / "TOCEURO4-2026-01-07-08-00-12-20260107"
    # ####################################################################################
    # # NCBJM
    # # NCBJM INCOMING
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "NCBJM" / "T11216072530091"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "NCBJM" / "T11216072530092"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "NCBJM" / "T11216072530094"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "NCBJM" / "T11216072530096"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "NCBJM" / "T11216072555312"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "NCBJM" / "T11216072555316"
    # # NCBJM OUTGOING
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "NCBJM" / "R06_CLNCBJ_JM_INTERNATIONALTRXNS_031795O_20250716_20250716_225832"
    # test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "NCBJM" / "R06_CLNCBJ_JM_JMDINTERNATIONALTRXNS_031796O_20250716_20250716_230003"
    test_path = PROJECT_ROOT / "persistence" / "files" / "landing" / "NCBJM" / "R06_CLNCBJ_JM_LOCALTRXNS_031792O_20250716_20250716_225706"
    # ####################################################################################

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

