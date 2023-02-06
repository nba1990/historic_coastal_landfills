# Functions, methods and utilities for various initial preprocessing steps in the AI pipeline.
# Concept: Data processing steps.
# Project: Historic Coastal Landfills
# Organisation: A Future Without Rubbish CIC, UK
# Author: Bharadwaj Raman
# Date First Authored: 04/02/2023

import multiprocessing
import operator
import time
import typing

import geopy.geocoders
import pandas
import pgeocode

import hcl_math.coordinates
from hcl_math.hcl_constants.constants import (
    NEW_COLS_ORDER,
    NUM_CORES_WITH_HT,
    NUM_CORES_WITHOUT_HT,
    MultiProcessingOptionsEnum,
    logger,
)
from timing.timer import MeasureTimer


def filter_dataset(
    hld_df: pandas.DataFrame,
    filter_column_name: str,
    filter_criteria: list,
    combination_operator: typing.Optional[typing.Callable],
) -> pandas.DataFrame:
    """Apply filtration criteria on the specified columns in the DataFrame."""
    starting_shape = hld_df.shape
    logger.info(
        f"Filtering dataset with initial shape: {starting_shape}, "
        f"filter_column_name: {filter_column_name}, filter_criteria: {filter_criteria}"
    )

    if combination_operator is not None:
        logger.info(f"combination_operator: {combination_operator.__name__}")

        combination_operator(
            hld_df[filter_column_name] == filter_criteria[0],
            hld_df[filter_column_name] == filter_criteria[1],
        )

        hld_df = hld_df[
            combination_operator(
                hld_df[filter_column_name] == filter_criteria[0],
                hld_df[filter_column_name] == filter_criteria[1],
            )
        ]

    # Remove the undocumented landfill site with NAN easting and northing
    hld_df = hld_df[(~hld_df["Easting"].isnull()) & (~hld_df["Northing"].isnull())]

    logger.info(
        f"Dataset shape after filtering: {hld_df.shape} out of {starting_shape}"
    )
    return hld_df


def extract_postcode_from_lat_long(
    latitude: float, longitude: float, enable_postcode_extraction: bool
) -> str:
    """Optionally extract postcode from latitude and longitude coordinates."""
    if enable_postcode_extraction:
        geolocator = geopy.geocoders.Nominatim(user_agent="geoapi_hcl")

        # Get the location information
        location = geolocator.reverse(f"{latitude}, {longitude}", exactly_one=True)
        # Get the address information
        if location is not None:
            address = location.raw["address"]
            # Get the postcode
            logger.info(f"Extracting postcode from address")
            postcode = address.get("postcode", "NA")
        else:
            address = "NA"
            # Get the postcode
            postcode = "NA"

    else:  # Disable postcode extraction from latitude and longitude
        address = "NA"
        # Get the postcode
        postcode = "NA"

    return postcode


def get_lat_long_postcode_from_easting_and_northing_single_process(
    hld_df: pandas.DataFrame, enable_postcode_extraction: bool
) -> tuple[list[float], list[float], list[str]]:
    """
    Convert easting and northing to latitude, longitude and optionally extract address and postcode from the
    coordinates using only a single simple linear process.
    """
    latitudes = []
    longitudes = []
    postcodes = []
    logger.info(
        "Using a simple single process only to convert easting and northing values to "
        "latitude and longitude - with optionally extracting postcode."
    )
    logger.info(
        f"Computation time might take significantly longer. "
        f"Consider using multiprocessing options if your hardware supports it."
    )
    logger.info(
        f"Supported multiprocessing options are: "
        f"{MultiProcessingOptionsEnum.MULTI_PROCESS_WITH_ONLY_PHYSICAL_CORES_NO_HT.name}: "
        f"{NUM_CORES_WITHOUT_HT} cores and | "
        f"{MultiProcessingOptionsEnum.MULTI_PROCESS_INCLUDING_LOGICAL_CORES_WITH_HT.name}: "
        f"{NUM_CORES_WITH_HT} cores respectively."
    )

    for index, (each_easting, each_northing) in enumerate(
        zip(hld_df["Easting"], hld_df["Northing"])
    ):
        logger.info(
            f"Converting easting and northing into latitude and longitude for site: {index} of "
            f"{hld_df.shape[0]}"
        )
        (
            latitude,
            longitude,
        ) = hcl_math.coordinates.convert_easting_northing_to_latitude_longitude(
            each_easting, each_northing
        )

        latitudes.append(latitude)
        longitudes.append(longitude)

        postcode = extract_postcode_from_lat_long(
            latitude, longitude, enable_postcode_extraction
        )
        postcodes.append(postcode)

    return latitudes, longitudes, postcodes


def get_lat_long_postcode_from_easting_and_northing_worker(
    row_index: int,
    dataset_len: int,
    easting: float,
    northing: float,
    enable_postcode_extraction: bool,
) -> tuple[int, float, float, str]:
    """
    Simple worker to convert easting and northing to latitude, longitude and optionally extract address and postcode
    from the coordinates. Used as a target in multiprocessing mode.
    """
    logger.info(
        f"Converting easting and northing into latitude and longitude for site: {row_index} of "
        f"{dataset_len}"
    )
    (
        latitude,
        longitude,
    ) = hcl_math.coordinates.convert_easting_northing_to_latitude_longitude(
        easting, northing
    )
    postcode = extract_postcode_from_lat_long(
        latitude, longitude, enable_postcode_extraction
    )

    return row_index, latitude, longitude, postcode


def get_lat_long_postcode_from_easting_and_northing_worker_wrapper(
    args,
) -> tuple[int, float, float, str]:
    """
    Wrap desired function of converting easting and northing to latitude, longitude and optionally extract postcode
    from the converted coordinates - sped up for multiprocessing use.
    """
    return get_lat_long_postcode_from_easting_and_northing_worker(*args)


def get_lat_long_postcode_from_easting_and_northing_multiple_processes(
    hld_df: pandas.DataFrame, enable_postcode_extraction: bool, num_cores: int
) -> tuple[list[float], list[float], list[str]]:
    """
    Convert easting and northing to latitude, longitude and optionally extract address and postcode from the
    coordinates using only a single simple linear process.
    """
    # Reference: https://towardsdatascience.com/multithreading-multiprocessing-python-180d0975ab29
    logger.info(
        "Using multiprocessing to parallelise computation of converting easting and northing values to "
        "latitude and longitude - with optionally extracting postcode"
    )
    logger.info(f"Using: {num_cores} to parallellise computation.")

    row_indices = []
    latitudes = []
    longitudes = []
    postcodes = []

    args_list = [
        (
            row_index,
            len(hld_df),
            hld_df["Easting"].values[row_index],
            hld_df["Northing"].values[row_index],
            enable_postcode_extraction,
        )
        for row_index in range(len(hld_df))
    ]

    with multiprocessing.Pool(processes=num_cores) as pool:
        results = pool.map(
            get_lat_long_postcode_from_easting_and_northing_worker_wrapper, args_list
        )

        for each_row_index, each_lat, each_long, each_postcode in results:
            row_indices.append(each_row_index)
            latitudes.append(each_lat)
            longitudes.append(each_long)
            postcodes.append(each_postcode)

    return latitudes, longitudes, postcodes


def get_lat_long_postcode_from_easting_and_northing(
    hld_df: pandas.DataFrame,
    enable_postcode_extraction: bool,
    multiprocessing_options: MultiProcessingOptionsEnum,
) -> pandas.DataFrame:
    """
    Convert easting and northing to latitude, longitude and optionally extract address and postcode from the
    coordinates either using a single simple linear process or using multiprocessing to speed up computation.
    """
    offline_nomi = pgeocode.Nominatim(country="GB")

    if enable_postcode_extraction:
        logger.info(
            f"Performing an online address lookup based on latitude and longitude"
        )
        logger.info(
            f"This free online service has usage limits, restrictions and rate limiting! "
            f"Process is intentionally slowed down so as to not get banned!"
        )
    else:
        logger.info(
            f"Skipping rate-limited online address lookup based on latitude and longitude"
        )

    with MeasureTimer() as measure_timer:
        if (
            multiprocessing_options
            == multiprocessing_options.SIMPLE_SINGLE_PROCESS_ONLY
        ):
            (
                latitudes,
                longitudes,
                postcodes,
            ) = get_lat_long_postcode_from_easting_and_northing_single_process(
                hld_df, enable_postcode_extraction
            )
        else:
            if (
                multiprocessing_options
                == multiprocessing_options.MULTI_PROCESS_WITH_ONLY_PHYSICAL_CORES_NO_HT
            ):
                num_cores = NUM_CORES_WITHOUT_HT
            elif (
                multiprocessing_options
                == multiprocessing_options.MULTI_PROCESS_INCLUDING_LOGICAL_CORES_WITH_HT
            ):
                num_cores = NUM_CORES_WITH_HT
            else:
                raise NotImplementedError
            (
                latitudes,
                longitudes,
                postcodes,
            ) = get_lat_long_postcode_from_easting_and_northing_multiple_processes(
                hld_df, enable_postcode_extraction, num_cores
            )

        hld_df["Latitude"] = latitudes
        hld_df["Longitude"] = longitudes
        hld_df["Postcode"] = postcodes

    # fmt: off
    logger.info(f"Conversion of easting and northing values to latitude and longitude coordinates "
                f"(with optionally extracting postcode information) execution took: "
                f"{time.strftime('%H:%M:%S', time.gmtime(measure_timer.elapsed_time))}")  # fmt: skip  # noqa
    # fmt: on

    return hld_df


def reorder_df_columns(hld_df: pandas.DataFrame) -> pandas.DataFrame:
    """Reorder dataset pandas DataFrame for easy lookup."""
    logger.info("Reordering columns in the dataframe")

    hld_df = hld_df[NEW_COLS_ORDER]
    return hld_df
