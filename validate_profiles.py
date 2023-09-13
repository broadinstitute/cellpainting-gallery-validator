"""Prepare files to be uploaded in S3"""
import re
from functools import partial
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map
import pandas as pd

from loader import load_barcode, load_plate
from jump.utils import get_logger, FEATURE_SET

logger = get_logger(__name__, "INFO")


def match_platemaps(batch: dict, remove_invalid=True):
    """Match platemaps with plates in batch"""
    barcode = load_barcode(batch)
    reg = re.compile(r"\.txt$")
    invalid_plates = []
    for plate in batch["plates"]:
        plate_id = plate["plate_id"]

        fname = barcode.query(f'Assay_Plate_Barcode=="{plate_id}"').Plate_Map_Name
        if len(fname) == 0:
            logger.warning(f"platemap not found for plate {plate_id}: {fname}")
            invalid_plates.append(plate)
            continue
        if len(fname) > 1:
            logger.warning(f"multiple platemaps for plate {plate_id}: {fname}")
            invalid_plates.append(plate)
            continue

        fname = fname.iloc[0]
        pmaps = []
        for pmap in batch["platemaps"]:
            if pmap["path"].endswith(fname):
                pmaps.append(pmap)
            elif reg.sub("", pmap["path"]).endswith(fname):
                pmaps.append(pmap)
        if len(pmaps) == 0:
            invalid_plates.append(plate)
            logger.warning(f"Missing platemap file for {plate_id}: {fname} not found")
            continue
        if len(pmaps) > 1:
            invalid_plates.append(plate)
            logger.warning(f"Multiple platemap files for {plate_id}: {pmaps}")
            continue

        plate["platemap"] = pmaps[0]
        plate["platemap_name"] = fname

    if remove_invalid and invalid_plates:
        logger.warning(f"removing {len(invalid_plates)} invalid plates")
        for plate in invalid_plates:
            batch["plates"].remove(plate)


COLUMN_SET = set(FEATURE_SET)


def check_profile(plate: dict, profile_key: str):
    """Check profile has all the feature columns.  Returns plate and result of
    the validation.
    """
    if profile_key in plate["profiles"]:
        profile = load_plate(plate, profile_key)
        columns = set(profile.columns)
        missing = list(COLUMN_SET - columns)
        valid = list(COLUMN_SET & columns)
        additional = list(columns - COLUMN_SET)
    else:
        missing, valid, additional = FEATURE_SET, [], []

    missing = pd.DataFrame({"feature": missing})
    missing["status"] = "missing"
    valid = pd.DataFrame({"feature": valid})
    valid["status"] = "valid"
    additional = pd.DataFrame({"feature": additional})
    additional["status"] = "additional"
    result = pd.concat([valid, missing, additional])
    result["plate_id"] = plate["plate_id"]

    return plate, result


def remove_invalid_profiles(dataset: dict, profile_key: str):
    """
    Remove plates that are not valid in-place. Return a pd.DataFrame with
    missing columns in the profiles.
    """
    par_func = partial(check_profile, profile_key=profile_key)
    removed = []
    for batch in tqdm(dataset["batches"], desc=dataset["dataset_id"]):
        output = process_map(par_func, batch["plates"], leave=False)
        plates = []
        for plate, result in output:
            if "missing" not in result["status"].values:
                plates.append(plate)
            result["batch_id"] = batch["batch_id"]
            removed.append(result)

        batch["plates"] = plates
    return pd.concat(removed)
