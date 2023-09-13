"""
Functions to parse `aws ls` output into python objects
"""
import re
import datetime
from jump.ptypes import S3Folder, ImageFolder, WorkspaceFolder, MetadataFolder
from jump.dao import Dataset, S3Object, Well, Batch

BLANK_RGX = re.compile(r" +")
DATE_RGX_POTS = (
    (re.compile(r".*(\d\d_\d\d_\d\d).*"), "%y_%m_%d"),
    (re.compile(r".*(\d{4}_\d\d_\d\d).*"), "%Y_%m_%d"),
    (re.compile(r".*(\d{8}).*"), "%Y%m%d"),
    (re.compile(r".*(\d{6}).*"), "%y%m%d"),
)


def extract_date(string, date_as_string=True):
    """
    Extract date from batch name. Prioritize '%Y_m_%d_BATCHNAME' and
    '%Ym%d_BATCHNAME' formats, else will search for the first match of  %y%m%d,
    %Y%m%d, %y_%m%_%d, or %y_%m%_%d
    """
    if date_as_string:
        return string
    for rgx, frmt in DATE_RGX_POTS:
        if match := rgx.match(string):
            date = datetime.datetime.strptime(match.group(1), frmt)
            return date
        # Could not parse the format. Try to find a %y%m%d, %Y%m%d,
        # %y_%m%_%d, or %y_%m%_%d in any part of the string
    # raise ValueError(f'Batch folder does not contain date: {string}')
    return None


def parse(line, date_as_string=True):
    """Parse a line of the `aws ls` command"""
    line = line.rstrip("\n")
    line = line.replace("//", "/")
    elems = BLANK_RGX.split(line, maxsplit=3)
    date_str, time_str, size, path = elems
    datetime_str = date_str + " " + time_str
    if date_as_string:
        date = datetime_str
    else:
        date = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    size = int(size)
    return date, size, path


datasets = {}


def get_dataset(s3_obj) -> Dataset:
    """Get (or create) a dataset"""
    elems = s3_obj.path.split("/")
    # elems[0] expected to be jump
    dataset_id = elems[1]
    if dataset_id not in datasets:
        datasets[dataset_id] = Dataset(dataset_id)
    return datasets[dataset_id]


def get_batch(batch_id, s3_obj: S3Object) -> Batch:
    """Get batch from a dataset"""
    dataset = get_dataset(s3_obj)
    if not dataset.has_batch(batch_id):
        # date = extract_date(batch_id)
        dataset.add_batch(batch_id)
    return dataset.get_batch(batch_id)


def process_line(line):
    """Create s3 object from a line"""
    date, size, path = parse(line)
    elems = path.split("/")
    # elems[0] expected to be jump
    s3_obj = S3Object(date, size, path)

    s3_folder = S3Folder(elems[2])  # 'images' or 'workspace'

    if s3_folder is S3Folder.IMAGE:
        batch_id = elems[3]  # e.g. 20210607_Batch_2
        payload = elems[4:]
        process_image(batch_id, payload, s3_obj)
    elif s3_folder is S3Folder.WORKSPACE:
        process_workspace(elems[3:], s3_obj)
    else:
        raise ValueError(f"Invalid path: {path}")


def process_image(batch_id, payload, s3_obj):
    """Process s3_obj from IMAGE folder"""
    batch = get_batch(batch_id, s3_obj)
    date = extract_date(batch_id)
    batch.date = date
    image_folder = ImageFolder(payload[0])
    if image_folder is ImageFolder.ILLUM:
        plate_id = payload[1]
        plate = batch.get_plate(plate_id)
        filename = "/".join(payload[2:])
        plate.correction.add_npy(s3_obj)
    elif image_folder is ImageFolder.IMAGE:
        filename = payload[1]
        plate_id = None

        # try first split with double '__', otherwise, with '_'
        tokens = filename.split("__")
        if len(tokens) == 1:
            tokens = filename.split("_")
        for token in tokens:
            # Avoid short ids. see
            # https://github.com/jump-cellpainting/aws/issues/75#issuecomment-1021746324
            if len(token) > 4:
                plate_id = token
                break
        if not plate_id:
            raise ValueError(f"Unable to find a valid plate_id in {filename}")
        plate = batch.get_plate(plate_id)
        plate.images.append(s3_obj)


def process_workspace(payload, s3_obj):
    """Process s3_obj from WORKSPACE folder"""
    workspace_folder = WorkspaceFolder(payload[0].lower())
    if workspace_folder is WorkspaceFolder.ANALYSIS:
        process_analysis(payload, s3_obj)
    elif workspace_folder is WorkspaceFolder.BACKEND:
        process_backend(payload, s3_obj)
    elif workspace_folder is WorkspaceFolder.LOAD_DATA:
        process_load_data(payload, s3_obj)
    elif workspace_folder is WorkspaceFolder.METADATA:
        process_metadata(payload, s3_obj)
    elif workspace_folder is WorkspaceFolder.PROFILES:
        process_profile(payload, s3_obj)


def process_outline(plate, filename, s3_obj):
    """
    Process outline files with format: A01_s1--cell_outlines.png or
    1086289792_P24_6_nuclei_outlines.png
    """
    basename = filename.split(".")[0]

    if "--" in basename:
        prefix, outline = basename.split("--")
        well_id, site_id = prefix.split("_s")  # site is prefixed by 's'
    else:
        plate_id, well_id, site_id, outline = basename.split("_", maxsplit=3)
        if plate_id != plate.plate_id:
            raise ValueError(
                f'Plate ID does not match: "{plate.plate_id}" != "{plate_id}"'
            )
    well = plate.get_well(well_id)
    site = well.get_site(site_id)
    if outline == "cell_outlines":
        site.cell_outline = s3_obj
    elif outline == "nuclei_outlines":
        site.nuclei_outline = s3_obj
    elif outline == "mito_outlines":
        site.mito_outline = s3_obj
    elif outline == "mito_obj":
        site.mito_obj_outline = s3_obj
    else:
        raise ValueError(f"Unknown outline: {s3_obj.path}")


def process_csv_file(plate, filename, s3_obj):
    """Process csv files in the analysis folder"""
    plate_id = plate.plate_id

    # Hack to support plate_ids with hyphens. add PLATE_ID as placeholder
    ids = filename.replace(plate_id, "PLATE_ID").split("-")
    ids = [id_.replace("PLATE_ID", plate_id) for id_ in ids]

    if len(ids) == 1:
        # Single ID: can be well or plate
        obj_id = ids[0]
        if Well.is_valid_id(obj_id):
            well = plate.get_well(obj_id)
            well.add_csv_file(s3_obj)
        else:
            if plate.plate_id not in obj_id:
                raise ValueError(f"Plate ID mismatch: {plate.plate_id} != {obj_id}")
            plate.add_csv_file(s3_obj)
        return

    plate_id, well_id = ids[:2]
    if plate.plate_id not in plate_id:
        raise ValueError(f"Plate ID mismatch: {plate.plate_id} != {plate_id}")
    well = plate.get_well(well_id)

    if len(ids) == 2:
        # grouped by well not by site. Well should own this csv file
        well.add_csv_file(s3_obj)
    elif len(ids) == 3:
        # grouped by site. Site should own this csv file
        site_id = ids[2]
        site = well.get_site(site_id)
        site.add_csv_file(s3_obj)
    else:
        raise ValueError(f'Invalid format for analysis folder in "{filename}"')


def process_analysis(payload, s3_obj):
    """Process files from analysis folder"""
    batch_id, plate_id = payload[1:3]
    batch = get_batch(batch_id, s3_obj)
    plate = batch.get_plate(plate_id)
    assert payload[3] == "analysis"
    if payload[5] == "outlines":
        filename = payload[6]
        process_outline(plate, filename, s3_obj)
    else:
        # It is any of the csv files.
        filename = payload[4]
        process_csv_file(plate, filename, s3_obj)


def process_backend(payload, s3_obj):
    """Process files from backend folder"""
    batch_id = payload[1]
    batch = get_batch(batch_id, s3_obj)
    plate_id = payload[2]
    plate = batch.get_plate(plate_id)
    if s3_obj.path.endswith("sqlite"):
        plate.backend_sqlite = s3_obj
    elif s3_obj.path.endswith("csv"):
        plate.backend_csv = s3_obj
    else:
        raise ValueError(f"Invalid backend value: {s3_obj.path}")


def process_load_data(payload, s3_obj):
    """Process files from load_data folder"""
    batch_id = payload[1]
    batch = get_batch(batch_id, s3_obj)
    plate_id = payload[2]
    plate = batch.get_plate(plate_id)
    if s3_obj.path.endswith("load_data.csv.gz") or s3_obj.path.endswith(
        "load_data.csv"
    ):
        plate.load_data_csv = s3_obj
    elif (
        s3_obj.path.endswith("load_data_with_illum.csv.gz")
        or s3_obj.path.endswith("load_data_with_illum.csv")
        or s3_obj.path.endswith("load_data_illum.csv.gz")
        or s3_obj.path.endswith("load_data_illum.csv")
    ):
        plate.load_data_with_illum = s3_obj
    elif "load_data_with_illum_split-" in s3_obj.path:
        # Ignore split illum files
        pass
    else:
        raise ValueError(f"Invalid load_data file: {s3_obj.path}")


def process_metadata(payload, s3_obj):
    """Process files from metadata folder"""
    metadata_folder = MetadataFolder(payload[1])
    if metadata_folder is MetadataFolder.EXTERNAL:
        dataset = get_dataset(s3_obj)
        dataset.add_metadata(s3_obj)
    elif metadata_folder is MetadataFolder.PLATEMAPS:
        batch_id = payload[2]
        batch = get_batch(batch_id, s3_obj)
        if payload[3] == "platemap":
            batch.platemaps.append(s3_obj)
        elif payload[3] == "barcode_platemap.csv":
            batch.barcode_platemap = s3_obj
        else:
            raise ValueError(f"Invalid platemap file: {s3_obj.path}")


def process_profile(payload, s3_obj):
    """Process files from profile folder"""
    if len(payload) < 3:
        raise ValueError(f"Object is not a profile: {s3_obj.path}")
    batch_id = payload[1]
    plate_id = payload[2]
    batch = get_batch(batch_id, s3_obj)
    plate = batch.get_plate(plate_id)
    plate.add_profile(s3_obj)
