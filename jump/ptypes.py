"""
Types in S3 paths
"""
from enum import Enum


class S3Folder(Enum):
    """Enum for root folder names"""

    IMAGE = "images"
    WORKSPACE = "workspace"


class ImageFolder(Enum):
    """Enum for image folder names"""

    ILLUM = "illum"
    IMAGE = "images"


class MetadataFolder(Enum):
    """Enum for metadata folder names"""

    EXTERNAL = "external_metadata"
    PLATEMAPS = "platemaps"


class WorkspaceFolder(Enum):
    """Enum for workspace folder names"""

    ANALYSIS = "analysis"
    BACKEND = "backend"
    LOAD_DATA = "load_data_csv"
    METADATA = "metadata"
    PROFILES = "profiles"
    QUALITY_CONTROL = "quality_control"
    QC = "qc"
    ASSAY_DEV = "assaydev"
    PIPELINE = "pipelines"
