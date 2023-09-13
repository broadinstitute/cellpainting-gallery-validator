"""
Validate schema
"""
import argparse
import re
from pathlib import Path
from collections import defaultdict
import orjson
from jsonschema.validators import validator_for
from validate_profiles import match_platemaps, remove_invalid_profiles
from jump.utils import get_logger

logger = get_logger(__name__, "INFO")


def create_validator(schema: dict, *args, **kwargs):
    """Create validator given a schema"""
    cls = validator_for(schema)
    cls.check_schema(schema)
    validator = cls(schema, *args, **kwargs)
    return validator


PLATE_RGX = re.compile(r"\$.batches\[\d+\].plates\[\d+\]")
BATCH_RGX = re.compile(r"\$.batches\[\d+\](\.platemaps|\.barcode_platemap|\.plates)?$")


def remove_invalid_elements(dataset, validator):
    """drop invalid elements in this json dataset according to the schema"""
    errors = list(validator.iter_errors(dataset))
    bad_plates = defaultdict(set)
    for error in errors:
        if PLATE_RGX.match(error.json_path):
            keys = error.absolute_path
            batch_ix, plate_ix = keys[1], keys[3]
            bad_plates[batch_ix].add(plate_ix)
            batch = dataset["batches"][batch_ix]
            plate = batch["plates"][plate_ix]
            logger.warning(
                f'Deleting invalid plate {batch["batch_id"]}.{plate["plate_id"]}: {error.json_path} {error.message}'
            )
    # Drop plates
    for batch_ix, plates_ix in bad_plates.items():
        plates = dataset["batches"][batch_ix]["plates"]
        plates = [plate for ix, plate in enumerate(plates) if ix not in plates_ix]
        dataset["batches"][batch_ix]["plates"] = plates

    bad_batches = set()
    errors = list(validator.iter_errors(dataset))
    for error in errors:
        if BATCH_RGX.match(error.json_path):
            keys = error.absolute_path
            batch_ix = keys[1]
            bad_batches.add(batch_ix)
            batch = dataset["batches"][batch_ix]
            logger.warning(
                f'Deleting invalid batch {batch["batch_id"]}: {error.json_path} {error.message}'
            )

    # Drop batches
    batches = dataset["batches"]
    batches = [batch for ix, batch in enumerate(batches) if ix not in bad_batches]
    dataset["batches"] = batches

    if dataset["batches"]:
        validator.validate(dataset)


def validate_dataset(jsonfile, outputfile, validator, check_profile):
    """Create new json files that complies schema.json and whose
    profiles are valid"""

    logger.info("Reading structure...")
    with open(jsonfile, "rb") as f_in:
        dataset = orjson.loads(f_in.read())
    logger.info(f"Validating {jsonfile}...")
    remove_invalid_elements(dataset, validator)
    for batch in dataset["batches"]:
        match_platemaps(batch)
    if check_profile:
        result = remove_invalid_profiles(dataset, "default")

        # Print counts
        counts = (
            result.query('status=="missing"').groupby("batch_id")["plate_id"].nunique()
        )
        missing_path = outputfile.parent / "missing_features.csv"
        result.to_csv(missing_path, index=False)
        if len(counts) > 0:
            logger.info("Plates removed per batch due to missing features.")
            print(counts)
            logger.info(f"{missing_path} generated.")

    with outputfile.open("wb") as f_out:
        f_out.write(orjson.dumps(dataset))
    logger.info(f"{outputfile} saved.")


def main():
    """Parse input params"""
    parser = argparse.ArgumentParser(
        description=(
            "Validate dataset complies schema.json and checks "
            "their profiles are valid"
        ),
    )
    parser.add_argument(
        "jsonfile", type=str, help="json file generated by create_structure script"
    )
    parser.add_argument(
        "--output",
        help="path to save the validated " "json. Default to same path as jsonfile",
    )
    parser.add_argument(
        "--schema",
        help="path to schema.json file. default: schema.json",
        default="./schema.json",
    )
    parser.add_argument(
        "--no-check-profiles",
        help="disable profile validation",
        dest="check_profile",
        action="store_false",
    )

    args = parser.parse_args()

    if not args.output:
        path = Path(args.jsonfile)
        args.output = path.parent / f"{path.stem}_validated.json"
    else:
        args.output = Path(args.output)
    if not args.check_profile:
        logger.warning("Skipping check profile consistency.")

    with open(args.schema, "rb") as f_in:
        validator = create_validator(orjson.loads(f_in.read()))
    validate_dataset(args.jsonfile, args.output, validator, args.check_profile)


if __name__ == "__main__":
    main()