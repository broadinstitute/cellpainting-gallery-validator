"""
Functions to browse the folders and load information
"""
from pathlib import Path
from functools import partial
import logging

import pandas as pd
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map
from jump.utils import CONFIG

import re

logger = logging.getLogger(__name__)

WELL_REGEX = re.compile(r"^([a-zA-Z]{1,2})([0-9]{1,2})$")


def s3_to_path(s3_obj: dict) -> Path:
    """Map s3 path to a local path in the `INPUTS_DIR` location"""
    s3_path = s3_obj["path"]
    _, suffix = s3_path.split(CONFIG["aws_prefix"])
    return Path(CONFIG["local_copy_path"]) / suffix


def normalize_well_position(series: pd.Series) -> pd.Series:
    """Normalize well position"""
    row = series.apply(lambda x: WELL_REGEX.match(x)[1])
    col = series.apply(lambda x: WELL_REGEX.match(x)[2])
    position = row.str.upper() + col.astype(int).apply(
        "{:02d}".format
    )  # pylint: disable=consider-using-f-string
    return position


def load_barcode(batch: dict) -> pd.DataFrame:
    """Get barcode list for a given batch"""
    if not batch["barcode_platemap"]:
        raise ValueError(f'missing barcode in {batch["batch_id"]}')

    barcode_path = s3_to_path(batch["barcode_platemap"])
    barcode = pd.read_csv(barcode_path, sep=",", dtype=str)
    if len(barcode) == 0:
        raise ValueError(f"{barcode_path} is empty")

    return barcode


def load_platemap(s3_obj: dict) -> pd.DataFrame:
    """Load platemap for a given plate"""
    platemap_path = s3_to_path(s3_obj)
    platemap = pd.read_csv(platemap_path, sep="\t", dtype=str)
    if len(platemap) == 0:
        raise ValueError(f"{platemap_path} platemap empty")
    platemap.rename({"well_position": "Metadata_Well"}, axis=1, inplace=True)
    platemap["Metadata_Well"] = normalize_well_position(platemap["Metadata_Well"])

    # Check for duplicates in wells
    if platemap["Metadata_Well"].duplicated().any():
        raise ValueError(f"{platemap_path} with duplicated wells")
    return platemap


def load_profile(s3_obj: dict) -> pd.DataFrame:
    """Load a profile given the s3_obj info"""
    path = s3_to_path(s3_obj)
    dtypes = {"Metadata_Plate": str, "Metadata_plate_map_name": str}
    profile = pd.read_csv(path, dtype=dtypes, low_memory=False)
    profile["Metadata_Well"] = normalize_well_position(profile["Metadata_Well"])

    # Check for duplicates in wells
    if profile["Metadata_Well"].duplicated().any():
        raise ValueError(f"{path} with duplicated wells")
    return profile


def load_plate(plate_props: dict, profile_key: str) -> pd.DataFrame:
    """Load profile and metadata from platemap"""
    platemap = load_platemap(plate_props["platemap"])
    profile = load_profile(plate_props["profiles"][profile_key])
    profile = pd.merge(profile, platemap, how="left", on="Metadata_Well")
    return profile.copy()


def load_batch(batch: dict, profile_key: str) -> pd.DataFrame:
    """Load all plates from a given batch"""

    par_func = partial(load_plate, profile_key=profile_key)
    plates = process_map(par_func, batch["plates"], leave=False)
    return plates


def load_source(batches, profile_key: str) -> pd.DataFrame:
    """Load all plates from a list of batches"""
    all_plates = []
    for batch_props in tqdm(batches, leave=False):
        batch_plates = load_batch(batch_props, profile_key)
        all_plates.extend(batch_plates)

    all_plates = pd.concat(all_plates)
    all_plates.reset_index(drop=True, inplace=True)
    return all_plates
