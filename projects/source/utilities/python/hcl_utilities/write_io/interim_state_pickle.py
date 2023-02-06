# Functions, methods and utilities for reading and writing programme internal intermediate states in pickle format.
# Concept: Persist intermediate saved states in python native pickle format.
# Project: Historic Coastal Landfills
# Organisation: A Future Without Rubbish CIC, UK
# Author: Bharadwaj Raman
# Date First Authored: 04/02/2023

import pathlib
import pickle

import pandas

from hcl_constants.constants import QUALIFIED_INTERMEDIATE_PICKLE_FILE, logger


def save_intermediate_state(
    hld_df: pandas.DataFrame,
    file_path: pathlib.Path = QUALIFIED_INTERMEDIATE_PICKLE_FILE,
):
    """Persist precious intermediate state to disk for quicker lookups."""
    logger.info(f"Saving intermediate state to file: {file_path}")
    with open(file_path, "wb") as fd:
        pickle.dump(hld_df, fd)


def read_intermediate_state(
    file_path: pathlib.Path = QUALIFIED_INTERMEDIATE_PICKLE_FILE,
) -> pandas.DataFrame:
    """Read from persisted intermediate state to speed up the analysis."""
    logger.info(f"Reading intermediate state from file: {file_path}")
    with open(file_path, "rb") as fd:
        hld_df = pickle.load(fd)
        return hld_df
