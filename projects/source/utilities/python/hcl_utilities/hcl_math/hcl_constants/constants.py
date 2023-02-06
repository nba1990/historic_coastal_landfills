# HCL Math Project level hcl_constants and variables that are used throughout the project and across related modules
import enum
import multiprocessing
import os
import pathlib

import loguru
import psutil

# fmt: off
NUM_CORES_WITH_HT = (multiprocessing.cpu_count())  # This always seems to include hyperthreading or virtual or logical cores  # noqa
NUM_CORES_WITHOUT_HT = psutil.cpu_count(logical=False)  # This only always gets the physical CPU core count
# fmt: on

logger = loguru.logger


class MultiProcessingOptionsEnum(enum.Enum):
    """Enumerator class for various supported multiprocessing options."""

    SIMPLE_SINGLE_PROCESS_ONLY = 0
    MULTI_PROCESS_WITH_ONLY_PHYSICAL_CORES_NO_HT = 1
    MULTI_PROCESS_INCLUDING_LOGICAL_CORES_WITH_HT = 2


USEFUL_COLS = [
    "Index",
    "HLD reference",
    "Site name",
    "Site address",
    "Licence ./ permit reference",
    "REGIS reference",
    "WRC reference",
    "BGS reference",
    "WRA reference",
    "Permit / licence holder",
    "Permit / licence holder address",
    "Site operator's name",
    "Site operator's address",
    "Site County",
    "Country",
    "Local Authority",
    "District/Unitary Council",
    "County Council",
    "Ward",
    "Constituency",
    "Region",
    "OS prefix",
    "Easting",
    "Northing",
    "Latitude",
    "Longitude",
    "Landfill Size",
    "Landfill Notes",
    "EA Region",
    "EA Area",
    "Licence issue date",
    "Licence surrender date",
    "First input date",
    "Last input date",
    "Inert Waste",
    "Industrial Waste",
    "Commercial Waste",
    "Household Waste",
    "Special / hazardous Waste",
    "Liquid / sludge Waste",
    "Waste unknown",
    "Gas control",
    "Leachate containment",
    "Exempt",
    "Licensed",
    "No licence required",
    "Buffer point",
    "New Update CE Property Jan 2023?",
    "Borders other site(s)",
    "Site(s) nearby",
    "Is study available?",
]

# Columns - New Order
NEW_COLS_ORDER = [
    "Index",
    "HLD reference",
    "Site name",
    "Site address",
    "Postcode",
    "Easting",
    "Northing",
    "Latitude",
    "Longitude",
    "Licence ./ permit reference",
    "REGIS reference",
    "WRC reference",
    "BGS reference",
    "WRA reference",
    "Permit / licence holder",
    "Permit / licence holder address",
    "Site operator's name",
    "Site operator's address",
    "Site County",
    "Country",
    "Local Authority",
    "District/Unitary Council",
    "County Council",
    "Ward",
    "Constituency",
    "Region",
    "OS prefix",
    "Landfill Size",
    "Landfill Notes",
    "EA Region",
    "EA Area",
    "Licence issue date",
    "Licence surrender date",
    "First input date",
    "Last input date",
    "Inert Waste",
    "Industrial Waste",
    "Commercial Waste",
    "Household Waste",
    "Special / hazardous Waste",
    "Liquid / sludge Waste",
    "Waste unknown",
    "Gas control",
    "Leachate containment",
    "Exempt",
    "Licensed",
    "No licence required",
    "Buffer point",
    "New Update CE Property Jan 2023?",
    "Borders other site(s)",
    "Site(s) nearby",
    "Is study available?",
]

CURRENT_DIR = pathlib.Path(os.getcwd())
PRIMARY_PARENT_PATH = CURRENT_DIR.parent.parent.parent.parent.parent.parent
PARENT_DATASET_PATH = PRIMARY_PARENT_PATH / "datasets"
DATASET_FILE_NAME = "UK_Historic_Landfill_Sites.xlsx"
QUALIFIED_DATASET_FILE = PARENT_DATASET_PATH / DATASET_FILE_NAME

INTERMEDIATE_PICKLE_FILE_NAME = (
    "saved_intermediate_states/hld_df_where_on_or_adjacent_ce_property_yes.pkl"
)
QUALIFIED_INTERMEDIATE_PICKLE_FILE = CURRENT_DIR / INTERMEDIATE_PICKLE_FILE_NAME
WASTE_FILTRATION_CRITERIA = [
    "Inert Waste",
    "Industrial Waste",
    "Commercial Waste",
    "Household Waste",
    "Special / hazardous Waste",
    "Liquid / sludge Waste",
    "Waste unknown",
    "Gas control",
    "Leachate containment",
    "Buffer point",
]
