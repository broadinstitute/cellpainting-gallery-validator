"""Main file"""
import argparse
from pathlib import Path
import pandas as pd
from tqdm.auto import tqdm

from jump.parsing import process_line, datasets
from jump import io
from jump.utils import get_logger

logger = get_logger(__name__, "INFO")


def parse_line(line):
    """Parse a line from the output of the `aws ls` command"""
    if "DS_Store" not in line:
        try:
            process_line(line)
        except ValueError as exc:
            return line.strip(), str(exc)
    return None


def parse_file(filepath: Path):
    """
    Read a text file containing the ouput of the `aws ls` command
    """
    logger.info("Reading File...")
    with filepath.open("r", 2) as fread:
        lines = fread.readlines()
    logger.info("Read completed.")

    logger.info("Parsing File...")
    errors = []
    for line in tqdm(lines, desc=filepath.stem):
        if error := parse_line(line):
            errors.append(error)
    logger.info("Parsing completed.")
    return errors


def process_file(list_file: str, output_dir: str):
    """method to process the aws list file"""
    filepath = Path(list_file)
    errors = parse_file(filepath)

    dataset_id = filepath.stem
    dirpath = Path(output_dir) / dataset_id
    dirpath.mkdir(parents=True, exist_ok=True)

    if len(errors) > 0:
        errors = pd.DataFrame(errors, columns=["path", "message_00"])
        errorpath = dirpath / "unknown_objects.csv"
        errors.to_csv(errorpath, index=False)

    if dataset_id not in datasets:
        logger.warning(f"Could not parse any line from {list_file} file")
        return

    jsonfile = dirpath / "structure.json"
    jsonext_file = dirpath / "structure_extensive.json"
    countsfile = dirpath / "image_counts.csv"
    io.to_json(datasets[dataset_id], jsonfile, jsonext_file, countsfile)
    datasets[dataset_id].clear()

    logger.info("Export completed")


def main():
    """Parse input params"""
    parser = argparse.ArgumentParser(
        description=(
            "Create structure.json files along with counts "
            "and errors while parsing the input"
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "list_file",
        type=str,
        help="text file containing the output of `aws ls` command.",
    )
    parser.add_argument(
        "--output_dir",
        default="./outputs",
        help="output directory to store the json files",
    )
    args = parser.parse_args()
    process_file(args.list_file, args.output_dir)


if __name__ == "__main__":
    main()
