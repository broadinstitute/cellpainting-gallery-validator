"""Create mappers from old codes to JCP2022 codes"""
import pandas as pd


def load_orf_mapper():
    """Load orf mapper"""
    # https://github.com/jump-cellpainting/jump-cellpainting/blob/jcporf/3.standardize/standardize_ksiling_jumpmoa_jumptarget2/data/05_release/2022_10_25_JUMP-CP_orf_library.csv
    codes = pd.read_csv("jcpids/2022_10_25_JUMP-CP_orf_library.csv")
    mapper = dict(zip(codes.broad_sample.values, codes.jcp2022_id.values))
    return mapper


ORF_MAPPER = load_orf_mapper()


def load_crispr_mapper():
    """Load crispr mapper"""
    # https://github.com/jump-cellpainting/jump-cellpainting/blob/4bd145565ebd6c91bc93e11d7456cc3086512ca8/3.standardize/standardization_crispr/data/05_release/2023_01_10_JUMP-CP_crispr_library.csv
    codes_1 = pd.read_csv("jcpids/2023_01_10_JUMP-CP_crispr_library.csv")
    # https://github.com/jump-cellpainting/jump-cellpainting/blob/f778f3998dab15997607061a44ad438ac0f1f560/3.standardize/standardization_crispr/data/05_release/2023_02_19_JUMP-CP_crispr_broad_library.csv
    codes_2 = pd.read_csv("jcpids/2023_02_19_JUMP-CP_crispr_broad_library.csv")
    codes = pd.concat([codes_1, codes_2])
    mapper = dict(zip(codes.broad_sample.values, codes.jcp2022_id.values))
    return mapper


CRISPR_MAPPER = load_crispr_mapper()


def load_cpjump1_mapper():
    """Load codes to map cpjump1 compounds"""
    # https://github.com/jump-cellpainting/jump-cellpainting/blob/master/0.design-pilots/output/compound_ID_cpjump1.csv
    cpd_library = pd.read_csv(
        "jcpids/2022_10_18_JUMP-CP_compound_library.csv", low_memory=False
    )
    cpjump1 = pd.read_csv("jcpids/compound_ID_cpjump1.csv")
    match = cpjump1.merge(cpd_library[["InChIKey", "jcp2022_id"]], on="InChIKey")
    match = match[["broad_id", "jcp2022_id"]]
    match = match.drop_duplicates().dropna()
    return match.set_index("broad_id")["jcp2022_id"].to_dict()


CPJUMP1_MAPPER = load_cpjump1_mapper()


def load_target1_mapper():
    """Load codes to map target1 compounds"""
    # https://github.com/jump-cellpainting/jump-cellpainting/blob/master/10.add-additional-target1-annotations/input/JUMP-Target-1_compound_metadata.tsv
    cpd_library = pd.read_csv(
        "jcpids/2022_10_18_JUMP-CP_compound_library.csv", low_memory=False
    )
    target1 = pd.read_csv("jcpids/JUMP-Target-1_compound_metadata.tsv", sep="\t")
    match = target1.merge(cpd_library[["InChIKey", "jcp2022_id"]], on="InChIKey")
    match = match[["broad_sample", "jcp2022_id"]]
    match = match.drop_duplicates().dropna()
    return match.set_index("broad_sample")["jcp2022_id"].to_dict()


TARGET1_MAPPER = load_target1_mapper()


def load_poscons_orf():
    """Load codes to map positive control compounds in orf plates"""
    # https://raw.githubusercontent.com/jump-cellpainting/jump-cellpainting/master/7.design-orf-experiment/input/poscon_wells.csv
    poscons = pd.read_csv("jcpids/poscon_wells.csv", index_col="platemap")
    poscons.fillna("untreated", inplace=True)
    return poscons


ORF_POSCONS = load_poscons_orf()


def update_orf_poscon(plate_props: dict, plate: pd.DataFrame):
    # https://github.com/jump-cellpainting/jump-cellpainting/issues/78#issuecomment-805942281
    """update in-place plate with positive control compounds in orf plates"""
    platemap_id = plate_props["platemap_name"]

    if platemap_id not in ORF_POSCONS.index:
        return

    poscons = ORF_POSCONS.loc[platemap_id]
    for well_pos, value in poscons.items():
        idx = plate["Metadata_Well"] == well_pos
        plate.loc[idx, "broad_sample"] = value

    plate["broad_sample"].fillna("untreated", inplace=True)


def names_to_jcp2022():
    """map perturbation names to jcp ids"""

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

    mapper_source_10 = {
        "Aloxistatin": "JCP2022_085227",
        "AMG 900": "JCP2022_037716",
        "Dexamethasone": "JCP2022_025848",
        "FK 866": "JCP2022_046054",
        "LY 2109761": "JCP2022_035095",
        "Quinidine": "JCP2022_050797",
        "TC-S 7004": "JCP2022_012818",
    }

    mapper_source_1 = {
        "C1": "JCP2022_085227",
        "C5": "JCP2022_035095",
        "C2": "JCP2022_037716",
        "C6": "JCP2022_064022",
        "C3": "JCP2022_025848",
        "C7": "JCP2022_050797",
        "C4": "JCP2022_046054",
        "C8": "JCP2022_012818",
    }

    mapper_source_2 = {
        "C1_ALOXISTATIN": "JCP2022_085227",
        "C5_LY2109761": "JCP2022_035095",
        "C2_AMG900": "JCP2022_037716",
        "C6_NVS-PAK1-1": "JCP2022_064022",
        "C3_DEXAMETHASONE": "JCP2022_025848",
        "C7_QUINIDINE": "JCP2022_050797",
        "C4_FK-866": "JCP2022_046054",
        "C8_TC-S-7004": "JCP2022_012818",
    }

    mapper_source_3 = {"LY109761": "JCP2022_035095"}

    mapper_source_5 = {"JCP2020_51616|JCP2020_59552": "JCP2022_007419"}

    # https://github.com/jump-cellpainting/aws/issues/80#issuecomment-1292332065
    mapper_unk_ids = {
        "JCP2020_30115": "JCP2022_087664",
        "JCP2020_27611": "JCP2022_022896",
    }

    # https://github.com/jump-cellpainting/jump-cellpainting/pull/137#issuecomment-1292108249
    mapper_bortezomib = {"BRD-K88510285-001-18-6": "JCP2022_028373"}

    mapper.update(mapper_source_1)
    mapper.update(mapper_source_2)
    mapper.update(mapper_source_3)
    mapper.update(mapper_source_5)
    mapper.update(mapper_source_10)
    mapper.update(mapper_unk_ids)
    mapper.update(mapper_bortezomib)

    return mapper


def load_master_jcp_mapper():
    """Load jcp mapper and add special codings"""
    # https://github.com/jump-cellpainting/jump-cellpainting/blob/master/3.standardize/standardize_ksiling_jumpmoa_jumptarget2/data/05_release/2022_10_18_JUMP-CP_compound_library.csv
    library = pd.read_csv(
        "jcpids/2022_10_18_JUMP-CP_compound_library.csv", low_memory=False
    )
    library = library.drop_duplicates(["jcp2020_id", "jcp2022_id"])
    mapper = dict(zip(library.jcp2020_id.values, library.jcp2022_id.values))
    mapper["DMSO"] = "JCP2022_033924"
    mapper["None"] = "JCP2022_999999"
    mapper["nan"] = "JCP2022_999999"
    mapper["unknown"] = "JCP2022_UNKNOWN"
    mapper.update(names_to_jcp2022())
    return mapper


JCP_MAPPER = load_master_jcp_mapper()
for c in ORF_MAPPER:
    if c not in JCP_MAPPER:
        JCP_MAPPER[c] = ORF_MAPPER[c]
    elif JCP_MAPPER[c] != ORF_MAPPER[c]:
        raise ValueError("Conflicting IDs between ORF_MAPPER and JCP libraries")

for c in CRISPR_MAPPER:
    if c not in JCP_MAPPER:
        JCP_MAPPER[c] = CRISPR_MAPPER[c]
    elif JCP_MAPPER[c] != CRISPR_MAPPER[c]:
        raise ValueError("Conflicting IDs between CRISPR_MAPPER and JCP libraries")

for c in CPJUMP1_MAPPER:
    if c not in JCP_MAPPER:
        JCP_MAPPER[c] = CPJUMP1_MAPPER[c]
    elif JCP_MAPPER[c] != CPJUMP1_MAPPER[c]:
        raise ValueError("Conflicting IDs between CPJUMP1_MAPPER and JCP libraries")

for c in TARGET1_MAPPER:
    if c not in JCP_MAPPER:
        JCP_MAPPER[c] = TARGET1_MAPPER[c]
    elif JCP_MAPPER[c] != TARGET1_MAPPER[c]:
        raise ValueError("Conflicting IDs between TARGET1_MAPPER and JCP libraries")
