"""
Fill NaN values according to GitHub issues documentation
"""
import pandas as pd
from id_mapping import update_orf_poscon
from loader import load_platemap


def is_target2(plate_props: dict, source_id: str) -> bool:
    """check if the given input is a target2 plate"""
    platemap_name = plate_props["platemap_name"]

    if source_id == "source_4":
        return platemap_name == "JUMP-Target-2_compound_platemap"

    if source_id == "source_5":
        return platemap_name.startswith("ACPJUM")

    if source_id == "source_11":
        return "LM" in platemap_name

    platemap = load_platemap(plate_props["platemap"])
    for _, vals in platemap.items():
        num_brd_codes = vals.fillna("").str.startswith("BRD").sum()
        if num_brd_codes / platemap.shape[0] > 0.5:
            return True
    return False


def _fillna_source_1(plate: pd.DataFrame, plate_props: dict):
    if is_target2(plate_props, "source_1"):
        plate["jump-identifier"].fillna("DMSO", inplace=True)
    elif "jump-identifier" in plate:
        # same logic as _fillna_source_6 but did not use poscon filling because
        # it didn't work as is
        # TODO: poscon filling might be needed
        nan_index = plate["jump-identifier"].isna()
        negcon = plate["control_type"] == "negcon"
        plate.loc[nan_index & negcon, "jump-identifier"] = "DMSO"
        plate["jump-identifier"].fillna("unknown", inplace=True)


def _fillna_source_2(plate: pd.DataFrame, plate_props: dict):
    if is_target2(plate_props, "source_2"):
        plate["jump-identifier"].fillna("DMSO", inplace=True)
    # https://github.com/jump-cellpainting/aws/issues/72#issuecomment-1326209857
    plate["jump-identifier"].fillna("unknown", inplace=True)


def _fillna_source_3(plate: pd.DataFrame, plate_props: dict):
    if is_target2(plate_props, "source_3"):
        plate["jump-identifier"].fillna("DMSO", inplace=True)
    # https://github.com/jump-cellpainting/aws/issues/73#issuecomment-1290289883
    plate["jump-identifier"].fillna("untreated", inplace=True)


def _fillna_source_4(plate: pd.DataFrame, plate_props: dict):
    # Assign poscon from ORF plates in source_4
    update_orf_poscon(plate_props, plate)
    platemap_name = plate_props["platemap_name"]
    if platemap_name == "control":
        # https://github.com/jump-cellpainting/jump-cellpainting/pull/137#issuecomment-1292073226
        plate["broad_sample"] = "untreated"
    elif platemap_name in ["bortezomib", "JUMP-Target-1_compound_platemap"]:
        # https://github.com/jump-cellpainting/datasets-private/issues/9#issuecomment-1292772545
        plate["broad_sample"].fillna("DMSO", inplace=True)
    elif platemap_name in [
        "JUMP-Target-1_crispr_platemap",
        "JUMP-Target-1_orf_platemap",
    ]:
        # https://github.com/jump-cellpainting/data-validation/pull/37#issuecomment-1431650705
        plate["broad_sample"].fillna("untreated", inplace=True)
    elif is_target2(plate_props, "source_4"):
        plate["broad_sample"].fillna("DMSO", inplace=True)


def _fillna_source_5(plate: pd.DataFrame, plate_props: dict):
    # https://github.com/jump-cellpainting/aws/issues/75#issuecomment-1287121698
    plate["jump-identifier"].fillna("untreated", inplace=True)


def _fillna_source_6(plate: pd.DataFrame, plate_props: dict):
    # https://github.com/jump-cellpainting/aws/issues/76#issuecomment-1291139430
    dmso_plates = [
        "110000293091",
        "110000294899",
        "110000294938",
        "110000296337",
        "110000296358",
        "110000296319",
        "110000296389",
        "110000295559",
        "110000296298",
        "110000295599",
        "110000295629",
        "110000295539",
        "110000295516",
        "110000296159",
        "110000297118",
        "110000297120",
    ]
    if plate_props["plate_id"] in dmso_plates:
        plate["jump-identifier"] = "DMSO"

    elif is_target2(plate_props, "source_6"):
        # In Target2 plates NaNs are DMSO
        plate.drop(columns=["jump-identifier"], errors="ignore", inplace=True)
        plate["broad_sample"].fillna("DMSO", inplace=True)

    # https://github.com/jump-cellpainting/aws/issues/76#issuecomment-1293275219
    elif "jump-identifier" in plate:
        nan_index = plate["jump-identifier"].isna()
        poscon = plate["control_type"] == "poscon"
        negcon = plate["control_type"] == "negcon"
        poscon_vals = plate.loc[nan_index & poscon, "pert_iname"].astype(str)
        plate.loc[nan_index & poscon, "jump-identifier"] = poscon_vals
        plate.loc[nan_index & negcon, "jump-identifier"] = "DMSO"

        plate["jump-identifier"].fillna("unknown", inplace=True)


def _fillna_source_8(plate: pd.DataFrame, plate_props: dict):
    # https://github.com/jump-cellpainting/aws/issues/78#issuecomment-1212198094
    if "jump-identifier" in plate:
        # Target2 plates don't have 'jump-identifier' but 'Metadatat_jump-identifier'
        plate["jump-identifier"].fillna("untreated", inplace=True)


def _fillna_source_9(plate: pd.DataFrame, plate_props: dict):
    # https://github.com/jump-cellpainting/aws/issues/79#issuecomment-1401625582
    plate["jump-identifier"].fillna("untreated", inplace=True)


def _fillna_source_10(plate: pd.DataFrame, plate_props: dict):
    # https://github.com/jump-cellpainting/aws/issues/80#issuecomment-1281212245
    plate["jump-identifier"].fillna("untreated", inplace=True)


def _fillna_source_11(plate: pd.DataFrame, plate_props: dict):
    plate["jump-identifier"].fillna("unknown", inplace=True)


def fillna(plate: pd.DataFrame, plate_props: dict, source_id: str):
    """Fill NaN depending on the source"""
    if source_id == "source_1":
        _fillna_source_1(plate, plate_props)
    if source_id == "source_2":
        _fillna_source_2(plate, plate_props)
    if source_id == "source_3":
        _fillna_source_3(plate, plate_props)
    if source_id == "source_4":
        _fillna_source_4(plate, plate_props)
    if source_id == "source_5":
        _fillna_source_5(plate, plate_props)
    if source_id == "source_6":
        _fillna_source_6(plate, plate_props)
    if source_id == "source_8":
        _fillna_source_8(plate, plate_props)
    if source_id == "source_9":
        _fillna_source_9(plate, plate_props)
    if source_id == "source_10":
        _fillna_source_10(plate, plate_props)
    if source_id == "source_11":
        _fillna_source_11(plate, plate_props)

    plate.dropna(axis=1, how="all", inplace=True)
