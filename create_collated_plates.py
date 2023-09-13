"""Create collated plate"""
import argparse
from itertools import combinations
import orjson
import pandas as pd
from nan_filling import is_target2

#
# Created with:
#     barcode_platemaps=$(aws s3 ls --recursive s3://cellpainting-gallery/jump/source_4/workspace/metadata/platemaps/ | grep barcode_platemap.csv|tr -s " "|cut -d" " -f4)
#     parallel aws s3 cp s3://cellpainting-gallery/{} - ::: $barcode_platemaps |grep Target
TARGET2_PLATES = [
    "BR00121436",
    "BR00121438",
    "BR00121439",
    "BR00121425",
    "BR00121428",
    "BR00121429",
    "BR00121430",
    "BR00121437",
    "BR00121424",
    "BR00121427",
    "BR00121423",
    "BR00127149",
    "BR00127147",
    "BR00127148",
    "BR00127145",
    "BR00127146",
    "BR00121426",
    "BR00126113",
    "BR00126114",
    "BR00126115",
    "BR00126116",
    "BR00126117",
]

TARGET1_PLATES = ["BR00125638", "BR00125181", "BR00123524", "BR00123523"]


def create_target2_list(dataset_ids: list[str], output_path) -> list[str]:
    """Create target 2 list using nan_filling.is_target2 heuristic"""
    target2 = []
    for dataset_id in dataset_ids:
        jsonfile = f"{output_path}/{dataset_id}/structure_validated.json"
        with open(jsonfile, "rb") as fread:
            dataset = orjson.loads(fread.read())
        for batch in dataset["batches"]:
            for plate_props in batch["plates"]:
                if is_target2(plate_props, dataset_id):
                    target2.append(plate_props["plate_id"])
    return target2


def find_platemap_groups(codes: pd.DataFrame) -> pd.Series:
    """find common platemaps based on (well, perturbation) tuples"""
    codes["well_pert"] = codes["Metadata_Well"] + "_" + codes["Metadata_JCP2022"]

    plates = codes.groupby("Metadata_Plate")["well_pert"].apply(set)
    platemaps = {}
    for i, set_i in plates.items():
        belongs = False
        for j in platemaps:
            set_j = plates[j]
            if set_i <= set_j:
                platemaps[j].add(i)
                belongs = True
            elif set_j < set_i:
                set_ = platemaps.pop(j)
                set_.add(i)
                platemaps[i] = set_
                belongs = True
            if belongs:
                break
        if not belongs:
            platemaps[i] = {i}
    platemaps = pd.Series(platemaps)
    return plates, platemaps


def find_target1(platemaps: pd.Series) -> list[str]:
    """Find TARGET1 plates"""
    for p in platemaps:
        if set(TARGET1_PLATES) <= p:
            return p
    return set()


def find_target2(
    platemaps: pd.Series, dataset_ids: list[str], output_path: str
) -> list[str]:
    """Find TARGET2 plates"""
    all_target2 = set()
    for p in platemaps:
        if set(TARGET2_PLATES) <= p:
            all_target2 = set(p)
    # Add missing Target2 from mapping process
    # TODO: Check this is correct
    all_target2 |= set(create_target2_list(dataset_ids, output_path))
    return all_target2


def find_dmso(plates: pd.DataFrame, platemaps: pd.Series) -> list[str]:
    """Find plates where every JCPID is JCP2022_033924"""
    dmso_id = "JCP2022_033924"

    all_dmso = []
    for i in platemaps.index:
        set_i = plates[i]
        if all(well_pert.endswith(dmso_id) for well_pert in set_i):
            all_dmso = platemaps[i]
    return all_dmso


def find_bortezomib(plates: pd.DataFrame, platemaps: pd.Series) -> list[str]:
    """Find plates where every JCPID is JCP2022_028373 or JCP2022_033924"""
    bortezomib_id = "JCP2022_028373"
    dmso_id = "JCP2022_033924"

    all_bortezomib = []
    for i in platemaps.index:
        set_i = plates[i]
        if all(
            well_pert.endswith(bortezomib_id) or well_pert.endswith(dmso_id)
            for well_pert in set_i
        ):
            all_bortezomib = platemaps[i]
    return all_bortezomib


def find_poscon8(plates: pd.DataFrame, platemaps: pd.Series) -> set[str]:
    """Find plates where every JCPID is any of the positive controls"""
    mapper = {
        "aloxistatin": "JCP2022_085227",
        "AMG900": "JCP2022_037716",
        "dexamethasone": "JCP2022_025848",
        "FK-866": "JCP2022_046054",
        "LY2109761": "JCP2022_035095",
        "NVS-PAK1-1": "JCP2022_064022",
        "quinidine": "JCP2022_050797",
        "TC-S-7004": "JCP2022_012818",
    }

    all_poscon8 = []
    poscon8_codes = set(mapper.values())
    for i in platemaps.index:
        set_i = map(lambda x: x.split("_", 1)[1], plates[i])
        set_codes = set(set_i)
        if set_codes <= poscon8_codes and len(set_codes) > 4:
            all_poscon8.extend(platemaps[i])
    all_poscon8 = set(all_poscon8)
    return all_poscon8


def find_compound_empty(
    plates: pd.DataFrame, platemaps: pd.Series, all_poscon8: set[str]
) -> set[str]:
    """Find plates where every JCPID is either positive control or
    DMSO/UNTREATED and are not in `all_poscon8`"""

    mapper = {
        "aloxistatin": "JCP2022_085227",
        "AMG900": "JCP2022_037716",
        "dexamethasone": "JCP2022_025848",
        "FK-866": "JCP2022_046054",
        "LY2109761": "JCP2022_035095",
        "NVS-PAK1-1": "JCP2022_064022",
        "quinidine": "JCP2022_050797",
        "TC-S-7004": "JCP2022_012818",
        "DMSO": "JCP2022_033924",
        "UNTREATED": "JCP2022_999999",
    }

    # Find Compound_empty
    all_compound_empty = []
    empty_codes = set(mapper.values())
    for i in platemaps.index:
        set_i = map(lambda x: x.split("_", 1)[1], plates[i])
        set_codes = set(set_i)
        if set_codes <= empty_codes and len(set_codes) > 4 and i not in all_poscon8:
            all_compound_empty.extend(platemaps[i])
    all_compound_empty = set(all_compound_empty)
    return all_compound_empty


def add_metadata_batch(meta: pd.DataFrame, output_path: str):
    """Add Metadata_Batch column inplace"""
    meta["Metadata_Batch"] = None
    dataset_ids = meta.Metadata_Source.unique()
    for dataset_id in dataset_ids:
        jsonfile = f"{output_path}/{dataset_id}/structure_validated.json"
        with open(jsonfile, "rb") as fread:
            dataset = orjson.loads(fread.read())
        for batch in dataset["batches"]:
            batch_id = batch["batch_id"]
            for plate_props in batch["plates"]:
                plate_id = plate_props["plate_id"]
                if plate_id in meta.index:
                    meta.loc[plate_id, "Metadata_Batch"] = batch_id


def build_metadata(output_path: str):
    """Build plate-based metadata with platetype and batch info"""
    collated_well_path = f"{output_path}/well.csv.gz"
    collated_plate_path = f"{output_path}/plate.csv.gz"
    codes = pd.read_csv(collated_well_path, dtype=str)
    dataset_ids = codes.Metadata_Source.unique()
    plates, platemaps = find_platemap_groups(codes)
    all_dmso = find_dmso(plates, platemaps)
    all_bortezomib = find_bortezomib(plates, platemaps)
    if all_bortezomib and all_dmso:
        all_bortezomib -= all_dmso
    all_target1 = find_target1(platemaps)
    all_target2 = find_target2(platemaps, dataset_ids, output_path)
    all_poscon8 = find_poscon8(plates, platemaps)
    all_compound_empty = find_compound_empty(plates, platemaps, all_poscon8)
    # Assert all lists are mutually exclusive
    all_lists = [
        all_bortezomib,
        all_dmso,
        all_target1,
        all_target2,
        all_poscon8,
        all_compound_empty,
    ]
    for list_a, list_b in combinations(all_lists, 2):
        if list_a and list_b:
            assert list_a.isdisjoint(list_b)

    # Create collated_metadata_plate
    meta = codes.drop_duplicates("Metadata_Plate").set_index("Metadata_Plate")
    meta = meta[["Metadata_Source"]].copy()

    meta["Metadata_PlateType"] = None
    meta.loc[list(all_bortezomib), "Metadata_PlateType"] = "BORTEZOMIB"
    meta.loc[list(all_dmso), "Metadata_PlateType"] = "DMSO"
    meta.loc[list(all_target1), "Metadata_PlateType"] = "TARGET1"
    all_target2 = all_target2 & set(meta.index)
    meta.loc[list(all_target2), "Metadata_PlateType"] = "TARGET2"
    meta.loc[list(all_poscon8), "Metadata_PlateType"] = "POSCON8"
    meta.loc[list(all_compound_empty), "Metadata_PlateType"] = "COMPOUND_EMPTY"

    # Find ORF
    orf_idx = (meta["Metadata_Source"] == "source_4") & meta[
        "Metadata_PlateType"
    ].isna()
    meta.loc[orf_idx, "Metadata_PlateType"] = "ORF"

    # Find CRISPR
    crispr_idx = (meta["Metadata_Source"] == "source_13") & meta[
        "Metadata_PlateType"
    ].isna()
    meta.loc[crispr_idx, "Metadata_PlateType"] = "CRISPR"

    # Fill others with COMPOUND
    meta["Metadata_PlateType"].fillna("COMPOUND", inplace=True)

    add_metadata_batch(meta, output_path)
    meta = meta.reset_index()
    meta = meta[
        ["Metadata_Source", "Metadata_Batch", "Metadata_Plate", "Metadata_PlateType"]
    ]
    meta.to_csv(collated_plate_path, index=False, compression="gzip")


def main():
    """Parse input params"""
    parser = argparse.ArgumentParser(
        description=(
            "Create the plate-based collated metadata file using collated well file"
        ),
    )
    parser.add_argument(
        "output_path", nargs="?", help="path for collated files", default="./outputs/"
    )
    args = parser.parse_args()
    build_metadata(args.output_path)


if __name__ == "__main__":
    main()
