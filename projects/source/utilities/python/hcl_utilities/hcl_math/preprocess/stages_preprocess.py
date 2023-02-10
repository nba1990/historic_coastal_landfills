# Functions, methods and utilities for various stage-wise preprocessing steps in the AI pipeline.
# Concept: Data stage-wise processing steps.
# Project: Historic Coastal Landfills
# Organisation: A Future Without Rubbish CIC, UK
# Author: Bharadwaj Raman
# Date First Authored: 04/02/2023
import copy
import pathlib
import typing

import pandas

from hcl_math.hcl_constants.constants import MultiProcessingOptionsEnum, logger
from hcl_math.preprocess.initial_preprocess import (
    filter_dataset,
    get_lat_long_postcode_from_easting_and_northing,
    reorder_df_columns,
)
from read_io.excel_io import read_dataset_to_df
from write_io.interim_state_pickle import save_intermediate_state


def run_first_stage(
    dataset_path: pathlib.Path,
    sheet_name: str,
    cols: list[int],
    filter_column_name: str,
    filter_criteria: list[str],
    combination_operator: typing.Optional[typing.Callable],
    enable_crs_conversion: bool,
    enable_postcode_extraction: bool,
    multiprocessing_options: MultiProcessingOptionsEnum,
    intermediate_state_file_path: pathlib.Path,
) -> pandas.DataFrame:
    """
    Run the first stage of the pipeline starting with reading dataset and ending with persisting internal state.

    Supported arguments/parameters and their examples:
    dataset_path: pathlib.Path = QUALIFIED_DATASET_FILE
    input_data_sheet_name: str = "Sites"
    cols: list[int]
    filter_column_name: str = "New Update CE Property Jan 2023?"
    primary_filter_criteria: list[str] = ["Yes", "Adjacent"]
    combination_operator: typing.Optional[typing.Callable] = [None, operator.or_, operator.and_]
    enable_postcode_extraction: bool = [True, False]
    multiprocessing_options: MultiProcessingOptionsEnum = [
        MultiProcessingOptionsEnum.SIMPLE_SINGLE_PROCESS_ONLY,
        MultiProcessingOptionsEnum.MULTI_PROCESS_WITH_ONLY_PHYSICAL_CORES_NO_HT,
        MultiProcessingOptionsEnum.MULTI_PROCESS_INCLUDING_LOGICAL_CORES_WITH_HT
        ]
    intermediate_state_file_path: pathlib.Path = QUALIFIED_INTERMEDIATE_PICKLE_FILE
    """
    logger.info("Running first stage of the pipeline.")
    hld_df = read_dataset_to_df(
        dataset_path=dataset_path, sheet_name=sheet_name, cols=cols
    )
    hld_df_filtered = filter_dataset(
        hld_df=hld_df,
        filter_column_name=filter_column_name,
        filter_criteria=filter_criteria,
        combination_operator=combination_operator,
    )

    if enable_crs_conversion:
        hld_df_filtered_enriched = get_lat_long_postcode_from_easting_and_northing(
            hld_df=hld_df_filtered,
            enable_postcode_extraction=enable_postcode_extraction,
            multiprocessing_options=multiprocessing_options,
        )

    else:
        hld_df_filtered_enriched = copy.deepcopy(hld_df_filtered)
        hld_df_filtered_enriched["Latitude"] = "NA"
        hld_df_filtered_enriched["Longitude"] = "NA"
        hld_df_filtered_enriched["Postcode"] = "NA"

    hld_df_filtered_enriched_reordered = reorder_df_columns(
        hld_df=hld_df_filtered_enriched
    )
    save_intermediate_state(
        hld_df=hld_df_filtered_enriched_reordered,
        file_path=intermediate_state_file_path,
    )

    logger.info("Finished first stage...")
    return hld_df_filtered_enriched_reordered
