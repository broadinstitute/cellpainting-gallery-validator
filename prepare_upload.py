"""
Script to create a folder to be synced in the cpg-0006 s3 folder
"""
import argparse
from pathlib import Path
from functools import partial
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map
import orjson
from loader import load_profile
from jump.utils import FEATURE_SET


def write_parquet(plate, dataset_id, batch_id, output_path):
    """write parquet file with the minimum set of columns"""
    plate_id = plate["plate_id"]
    filepath = (
        f"{output_path}/{dataset_id}/workspace/profiles/"
        f"{batch_id}/{plate_id}/{plate_id}.parquet"
    )
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    dframe = load_profile(plate["profiles"]["default"])
    dframe["Metadata_Source"] = dataset_id
    dframe["Metadata_Plate"] = dframe["Metadata_Plate"].astype(str)
    dframe = dframe[
        ["Metadata_Source", "Metadata_Plate", "Metadata_Well"] + FEATURE_SET
    ]
    dframe.to_parquet(filepath, index=False)


def write_dataset(jsonfile, output_path):
    """Write dataset"""
    with open(jsonfile, "rb") as f_in:
        dataset = orjson.loads(f_in.read())
    dataset_id = dataset["dataset_id"]
    for batch in tqdm(dataset["batches"], leave=False, desc=dataset_id):
        batch_id = batch["batch_id"]
        par_func = partial(
            write_parquet,
            dataset_id=dataset_id,
            batch_id=batch_id,
            output_path=output_path,
        )
        process_map(par_func, batch["plates"], leave=False)


def main():
    """Parse input params"""
    parser = argparse.ArgumentParser(description="Prepare folder to by synced in aws")
    parser.add_argument(
        "jsonfile",
        help="validated json file created by created_validated_schema.py script",
    )
    parser.add_argument(
        "--output",
        default="./outputs/clean",
        help="output dir to write the parquet files",
    )

    args = parser.parse_args()

    write_dataset(args.jsonfile, args.output)


if __name__ == "__main__":
    main()
