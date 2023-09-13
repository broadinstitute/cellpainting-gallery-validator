"""
Read and write objects
"""
import orjson
import pandas as pd
from jump import dao
from jump.utils import get_logger

logger = get_logger(__name__, "INFO")


def dropna(obj):
    """Recursively remove None objects"""
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(dropna(x) for x in obj if x is not None)
    if isinstance(obj, dict):
        return type(obj)(
            (dropna(k), dropna(v))
            for k, v in obj.items()
            if k is not None and v is not None
        )
    return obj


def serialize_plate(plate: dao.Plate) -> dict:
    """Serialize plate with wells and images"""
    plate_props = plate.to_dict()

    wells = []
    for well in plate.wells.values():
        sites = [site.to_dict() for site in well.sites.values()]
        well_props = well.to_dict()
        well_props["sites"] = sites
        wells.append(well_props)
    plate_props["wells"] = wells
    plate_props["images"] = plate.images
    return plate_props


def to_json(dataset: dao.Dataset, jsonfile, jsonext_file, countsfile):
    """Export a dataset to json file"""

    structure = {"dataset_id": dataset.dataset_id, "batches": []}
    counts = []
    for batch in dataset.batches:
        plates = []
        for plate_id, plate in batch.plates.items():
            counts.append(
                {
                    "dataset_id": dataset.dataset_id,
                    "batch_id": batch.batch_id,
                    "plate_id": plate_id,
                    "num_images": len(plate.images),
                }
            )
            plates.append(serialize_plate(plate))

        batch_props = batch.to_dict()
        batch_props["plates"] = plates
        structure["batches"].append((batch_props))

    logger.info("dropping None values...")
    structure = dropna(structure)
    logger.info("writing structure_extensive object...")
    with open(jsonext_file, "wb") as fwriter:
        fwriter.write(orjson.dumps(structure))
    logger.info("structure_extensive object saved.")

    # remove wells and images to get a smaller json
    for batch in structure["batches"]:
        for plate_props in batch["plates"]:
            del plate_props["wells"]
            del plate_props["images"]
    with open(jsonfile, "wb") as fwriter:
        fwriter.write(orjson.dumps(structure))

    counts = pd.DataFrame(counts)
    counts.to_csv(countsfile, index=False)
