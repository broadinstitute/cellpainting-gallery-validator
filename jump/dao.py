"""
Validator for s3 object in JUMP project
"""
import re
import datetime
from dataclasses import dataclass
from jump.utils import CONFIG

# pylint: disable=too-few-public-methods


class CSVAnalysisContainer:
    """Either a Plate, Well or a Site containing several CSV files related to
    the analysis folder"""

    FILES = CONFIG["analysis_csv_files"]
    VALID_EXT = "csv", "csv.gz"

    def __init__(self):
        self.csv_files = {}

    def add_csv_file(self, s3_obj):
        """
        Add csv path to this Site object
        """
        path = s3_obj.path
        if not any(map(path.endswith, self.VALID_EXT)):
            raise ValueError("Invalid extension for a CSV file")

        for file in self.FILES:
            valid_suffix = [f"{file}.{ext}" for ext in self.VALID_EXT]
            if any(map(path.endswith, valid_suffix)):
                if file in self.csv_files:
                    raise ValueError(
                        f"Duplicated csv file for {file}: {self.csv_files[file]}, {path}"
                    )
                self.csv_files[file] = s3_obj
                return
        raise ValueError(f"csv_file not valid in analysis folder: {path}")


@dataclass
class S3Object:
    """S3 object to be validated"""

    date: datetime.datetime
    size: int
    path: str

    def __str__(self):
        return self.path


class Batch:
    """Object representing batches"""

    def __init__(self, batch_id):
        """Create batch

        :batch_id: TODO
        :returns: TODO
        """
        self.batch_id = batch_id
        self.plates = {}
        self.platemaps = []
        self.barcode_platemap = None

    def find_one_plate(self, path):
        """Find the plate whose ID is in the given path.
        If there are more than one coincidences it raises an error"""
        match = None
        for plate_id, plate in self.plates.items():
            if plate_id in path:
                if match:
                    raise ValueError(
                        f"There is more than one plate_id that matches this path: {path}"
                    )
                match = plate
        return match

    def get_plate(self, plate_id):
        """Return a plate object given a plate_id"""
        # if plate := self.find_one_plate(plate_id):
        # return plate
        if plate_id in self.plates:
            return self.plates[plate_id]
        if Well.is_valid_id(plate_id):
            raise ValueError("Expecting plate_id, receive well_id")
        plate = Plate(plate_id)
        self.plates[plate_id] = plate
        return plate

    def to_dict(self):
        """Serialize this batch in a shallow dict"""
        return {
            "batch_id": self.batch_id,
            "platemaps": self.platemaps,
            "barcode_platemap": self.barcode_platemap,
        }


class Plate(CSVAnalysisContainer):  # pylint: disable=too-many-instance-attributes
    """Plate"""

    def __init__(self, plate_id):
        self.correction = IllumCorrection()
        self.images = []
        self.date = None
        self.wells = {}
        self.plate_id = plate_id
        self.backend_csv = None
        self.backend_sqlite = None
        self.load_data_with_illum = None
        self.load_data_csv = None
        self.profiles = {}
        super().__init__()

    def get_well(self, well_id):
        """Return the well in this plate"""
        if well_id not in self.wells:
            if Well.is_valid_id(well_id):
                self.wells[well_id] = Well(well_id)
            else:
                raise ValueError(f"Invalid Well Name {well_id}")
        return self.wells[well_id]

    def add_profile(self, s3_obj):
        """
        Add profile path to this plate
        """
        path = s3_obj.path
        for profile in CONFIG["profiles"]:
            if path.endswith(f"{self.plate_id}_{profile}.csv.gz") or path.endswith(
                f"{self.plate_id}_{profile}.csv"
            ):

                if profile in self.profiles:
                    raise ValueError(
                        f"Duplicated profile for {profile}: {self.profiles[profile]}, {path}"
                    )
                self.profiles[profile] = s3_obj
                return
            if path.endswith(f"{self.plate_id}.csv.gz") or path.endswith(
                f"{self.plate_id}.csv"
            ):

                profile = "default"
                if profile in self.profiles:
                    raise ValueError(
                        f"Duplicated profile for {profile}: {self.profiles[profile]}, {path}"
                    )
                self.profiles[profile] = s3_obj
                return
        raise ValueError(f"Profile is not valid for plate {self.plate_id}: {path}")

    def to_dict(self):
        """Serialize this plate in a shallow dict"""
        props = {
            "plate_id": self.plate_id,
            "backend_csv": self.backend_csv,
            "backend_sqlite": self.backend_sqlite,
            "load_data_with_illum": self.load_data_with_illum,
            "load_data_csv": self.load_data_csv,
            "correction_files": self.correction.resources,
            "profiles": self.profiles,
        }

        if self.csv_files:
            props["containers"] = self.csv_files
        return props


class Well(CSVAnalysisContainer):
    """Well"""

    ID_REGEX = re.compile(r"^[a-zA-Z]{1,2}\d\d$")

    def __init__(self, well_id):
        self.well_id = well_id
        self._sites = {}
        super().__init__()

    def get_site(self, site_id):
        """Get site from well"""
        if site_id not in self._sites:
            self._sites[site_id] = Site(site_id)
        return self._sites[site_id]

    @property
    def sites(self):
        """get all sites for this well"""
        # Complete site csvfiles with well files when missing.
        for site in self._sites.values():
            for file in CSVAnalysisContainer.FILES:
                if file not in site.csv_files and file in self.csv_files:
                    site.csv_files = self.csv_files
        return self._sites

    def to_dict(self):
        """Serialize this well in a shallow dict"""
        props = {
            "well_id": self.well_id,
            # 'num_sites': len(self._sites)
        }
        if self.csv_files:
            props["containers"] = self.csv_files
        return props

    @staticmethod
    def is_valid_id(well_id):
        """Check if the well_id is valid"""
        return Well.ID_REGEX.match(well_id)


class Site(CSVAnalysisContainer):
    """Site"""

    def __init__(self, site_id):
        self.site_id = site_id
        self.cell_outline = None
        self.nuclei_outline = None
        self.mito_outline = None
        self.mito_obj_outline = None
        super().__init__()

    def to_dict(self):
        """Serialize this site in a shallow dict"""
        props = {
            "site_id": self.site_id,
            "cell_outline": self.cell_outline,
            "nuclei_outline": self.nuclei_outline,
            "mito_outline": self.mito_outline,
            "mito_obj_outline": self.mito_outline,
        }
        if self.csv_files:
            props["containers"] = self.csv_files
        return props


class IllumCorrection:
    """
    Illumination Correction paths
    """

    CHANNELS = CONFIG["illumination_channels"]

    def __init__(self):
        self.resources = {}

    def add_npy(self, s3_obj):
        """
        Add npy path to this Illumination correction object
        """
        path = s3_obj.path
        if not path.endswith(".npy"):
            raise ValueError(
                "Invalid format for Illumination Correction. Expected .npy"
            )
        for channel in self.CHANNELS:
            if path.endswith(f"{channel}.npy"):
                if channel in self.resources:
                    raise ValueError(
                        f"Duplicated correction for {channel}: {self.resources[channel]}, {path}"
                    )
                self.resources[channel] = s3_obj
                return
        raise ValueError(f"Channel is not valid for Illumination: {path}")


class Dataset:
    """Object representing all the data from a source"""

    def __init__(self, dataset_id):
        self.dataset_id = dataset_id
        self._metadata = []
        self._batches = {}

    def add_batch(self, batch_id):
        """Add a new batch for this dataset"""
        if batch_id in self._batches:
            raise ValueError(f"Batch already exists: {batch_id}")
        self._batches[batch_id] = Batch(batch_id)

    def get_batch(self, batch_id):
        """Get batch obj from ID"""
        return self._batches[batch_id]

    def add_metadata(self, s3_obj: S3Object):
        """Add a new metadata resource"""
        self._metadata.append(s3_obj)

    def has_batch(self, batch_id):
        """Check if batch exists in this dataset"""
        return batch_id in self._batches

    @property
    def batches(self):
        """Return all batches"""
        return list(self._batches.values())

    def clear(self):
        """Remove all data"""
        self._batches.clear()
        self._metadata.clear()
