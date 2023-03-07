# Demo python file for explaining maths concepts easily
# Concept: Permutations nPr and Combinations nCr problem
# Project: Historic Coastal Landfills
# Organisation: A Future Without Rubbish CIC, UK
# Author: Bharadwaj Raman
# Date First Authored: 27/01/2023

# Import modules
import itertools
import operator
import pathlib
import time
import typing

import numpy
import pandas

from hcl_math.hcl_constants.constants import (
    QUALIFIED_DATASET_FILE,
    QUALIFIED_INTERMEDIATE_PICKLE_FILE,
    USEFUL_COLS,
    WASTE_FILTRATION_CRITERIA,
    MultiProcessingOptionsEnum,
    logger,
)
from hcl_math.plot_filter_combinations import plot_count_statistics_filter_combinations
from hcl_math.preprocess.stages_preprocess import run_first_stage
from read_io.excel_io import (
    convert_useful_col_names_to_col_letters_and_indices,
    load_excel_column_headers,
)
from timing.timer import MeasureTimer
from write_io.excel_io import save_dataframe_to_existing_worksheet
from write_io.interim_state_pickle import read_intermediate_state

EXCEL_COL_HEADERS, EXCEL_COL_LETTERS, EXCEL_COL_INDICES = load_excel_column_headers(
    QUALIFIED_DATASET_FILE, 0
)
WASTE_CRITERIA_COL_INDICES_NUM = [
    EXCEL_COL_INDICES[EXCEL_COL_HEADERS.index(criteria)]
    for criteria in WASTE_FILTRATION_CRITERIA
]

CHECK_COLUMNS = [EXCEL_COL_HEADERS[i] for i in WASTE_CRITERIA_COL_INDICES_NUM]
numpy.testing.assert_array_equal(CHECK_COLUMNS, WASTE_FILTRATION_CRITERIA)

WASTE_CRITERIA_COL_INDICES_CHAR = [
    EXCEL_COL_LETTERS[i] for i in WASTE_CRITERIA_COL_INDICES_NUM
]
(
    USEFUL_COLUMN_LETTERS,
    USEFUL_COLUMN_NUMS,
) = convert_useful_col_names_to_col_letters_and_indices(
    USEFUL_COLS, EXCEL_COL_HEADERS, EXCEL_COL_LETTERS, EXCEL_COL_INDICES
)
STATS_DF_OUTPUT_COLS = [
    "Experiment Index",
    "Primary CE property filter",
    "Waste Filter Criteria",
    "Filter Order (r Value in nCr)",
    "Num sites",
    "Corresponding site ref nums",
    "Num unique site refs per primary filter per Filter Order",
    "Corresponding site ref nums per primary filter per Filter Order",
    "Total num unique site refs",
    "Corresponding total unique site ref nums",
]


def get_filter_combinations_criteria_multiple_orders() -> tuple[
    list[list[tuple[int, ...]]],
    list[list[tuple[str, ...]]],
    list[list[tuple[str, ...]]],
]:
    """Combine filter criteria of multiple orders using nCr - where order does not matter."""
    column_indices_num_combs = []
    column_indices_char_combs = []
    filtration_criteria_combs = []
    combined_filters_lens = []
    logger.info(
        f"$$$$$$$$$$$$$$$$$$$$ COMBINATIONS FOR CRITERIA: {len(WASTE_FILTRATION_CRITERIA)} $$$$$$$$$$$$$$$$$$$$"
    )
    for indx in range(1, len(WASTE_FILTRATION_CRITERIA) + 1):
        logger.info(f"------ Combining filter criteria with order r: {indx} ------")
        column_indices_num_comb = list(
            itertools.combinations(WASTE_CRITERIA_COL_INDICES_NUM, indx)
        )
        column_indices_num_combs.append(column_indices_num_comb)

        column_indices_char_comb = list(
            itertools.combinations(WASTE_CRITERIA_COL_INDICES_CHAR, indx)
        )
        column_indices_char_combs.append(column_indices_char_comb)

        filtration_criteria_comb = list(
            itertools.combinations(WASTE_FILTRATION_CRITERIA, indx)
        )
        filtration_criteria_combs.append(filtration_criteria_comb)

        combined_filters_len = len(column_indices_num_combs[indx - 1])
        combined_filters_lens.append(combined_filters_len)
        logger.info(
            f"###### Number of combined filter criteria: {combined_filters_len}"
        )

    logger.info(
        f"###### Total Number of combined filter criteria: {sum(combined_filters_lens)}"
    )
    return (
        column_indices_num_combs,
        column_indices_char_combs,
        filtration_criteria_combs,
    )


def get_filter_permutations_criteria_multiple_orders() -> tuple[
    list[list[int]], list[list[str]], list[list[str]]
]:
    """Permute filter criteria of multiple orders using nPr - where order matters."""
    column_indices_num_perms = []
    column_indices_char_perms = []
    filtration_criteria_perms = []
    permuted_filters_lens = []
    logger.info(
        f"$$$$$$$$$$$$$$$$$$$$ PERMUTATIONS FOR CRITERIA: {len(WASTE_FILTRATION_CRITERIA)}  $$$$$$$$$$$$$$$$$$$$"
    )
    for indx in range(1, len(WASTE_FILTRATION_CRITERIA) + 1):
        logger.info(f"------ Permuting filter criteria with order r: {indx} ------")
        column_indices_num_perm = list(
            itertools.permutations(WASTE_CRITERIA_COL_INDICES_NUM, indx)
        )
        column_indices_num_perms.append(column_indices_num_perm)

        column_indices_char_perm = list(
            itertools.permutations(WASTE_CRITERIA_COL_INDICES_CHAR, indx)
        )
        column_indices_char_perms.append(column_indices_char_perm)

        filtration_criteria_perm = list(
            itertools.permutations(WASTE_FILTRATION_CRITERIA, indx)
        )
        filtration_criteria_perms.append(filtration_criteria_perm)

        permuted_filters_len = len(column_indices_num_perms[indx - 1])
        permuted_filters_lens.append(permuted_filters_len)
        logger.info(
            f"###### Number of permuted filter criteria: {permuted_filters_len}"
        )

    logger.info(
        f"###### Total Number of permuted filter criteria: {sum(permuted_filters_lens)}"
    )
    return (
        column_indices_num_perms,
        column_indices_char_perms,
        filtration_criteria_perms,
    )


def load_initial_filtered_data(
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
    load_existing: bool = True,
) -> pandas.DataFrame:
    """
    Load initial filtered data by reusing and repurposing existing (modified) mechanisms.
    :param dataset_path:
    :param sheet_name:
    :param cols:
    :param filter_column_name:
    :param filter_criteria:
    :param combination_operator:
    :param enable_crs_conversion:
    :param enable_postcode_extraction:
    :param multiprocessing_options:
    :param intermediate_state_file_path:
    :param load_existing:
    :return:
    """
    if load_existing:
        if not intermediate_state_file_path.exists():
            hld_df_filtered_enriched_reordered = run_first_stage(
                dataset_path,
                sheet_name,
                cols,
                filter_column_name,
                filter_criteria,
                combination_operator,
                enable_crs_conversion,
                enable_postcode_extraction,
                multiprocessing_options,
                intermediate_state_file_path,
            )

        else:
            hld_df_filtered_enriched_reordered = read_intermediate_state(
                file_path=intermediate_state_file_path
            )

    else:  # Run the first stage regardless of pre-existing intermediate state
        hld_df_filtered_enriched_reordered = run_first_stage(
            dataset_path,
            sheet_name,
            cols,
            filter_column_name,
            filter_criteria,
            combination_operator,
            enable_crs_conversion,
            enable_postcode_extraction,
            multiprocessing_options,
            intermediate_state_file_path,
        )

    return hld_df_filtered_enriched_reordered


def get_filter_criteria_counts(
    hld_data_df: pandas.DataFrame,
    filter_orders_combinations_criteria: list[list[tuple]],
) -> tuple[list[dict[str, list]], list[dict[str, list]]]:
    """
    Calculate count statistics and their corresponding site reference numbers for various filter combinations.

    Note: The filter combinations may not mutually exclusive when they are applied on the landfill sites.
    What this means is that, for example, some landfill sites may contain Inert Waste and some may contain Household
    Waste. Some of these sites may contain both (meaning, the presence of Inert Waste does not automatically exclude
    the same sites from containing Household Waste as well).

    ---------- Example: ----------
    Imagine that there is large bowl full of 100 various lego pieces of different colours and shapes.
    Imagine that there are 2 major categories you'd like to classify the lego pieces by, namely: ["Colour", Shape"]

    Category 1: Colour: You could classify and calculate the distribution of the lego pieces on the basis of just colour
    say, red or blue. Let's say that this value is 42. Imagine that the rest of the 58 pieces are different colours say,
    green or violet and are not considered as part of our filtration process. Note that (42 + 58) = 100.

    Category 2: Shape: You could also classify and calculate the distribution of the lego pieces on the basis of just
    shapes say, (cubical or spherical). Let's say that this value is 84. Imagine that the rest of the 16 pieces are
    different shapes say, tetrahedron or L-shape and are not considered as part of our filtration process.
    Note that (84 + 16) = 100.

    You can't just add the totals from category 1 and 2 as that would give nonsensical results.
    In this example, (42 + 84) = 126 - which is greater than the total number of lego pieces in the bowl (100).

    The reason for this discrepancy is that while the individual category counts make sense on their own,
    their summation does not - due to the fact that some pieces can be both red and cubical or blue and spherical
    and vice versa.
    Just because a lego piece is red does not automatically exclude it from being spherical.
    Likewise, just because a lego piece is cubical does not automatically exclude it from being red.
    Both of these are merely various simultaneous properties or characteristics of the lego piece under observation
    and hence cannot be used to meaningfully "split" the pieces so that they add up to the total 100.

    :param hld_data_df:
    :param filter_orders_combinations_criteria:
    :return:
    """
    filter_counts = []
    site_ref_nums = []
    # One Hot Encode to 0 and 1 for easier comparison and filtering
    hld_data_df_filtered = hld_data_df[WASTE_FILTRATION_CRITERIA].eq("Yes").astype(int)
    hld_data_df_filtered["HLD reference"] = hld_data_df["HLD reference"]
    # for filter_order in range(1, len(WASTE_FILTRATION_CRITERIA) + 1):
    #     filter_counts[filter_order] = {}
    #     for filter_combination in itertools.combinations(
    #         WASTE_FILTRATION_CRITERIA, filter_order
    #     ):
    #         filter_count = (
    #             hld_data_df_filtered[list(filter_combination)].sum(axis=1)
    #             == filter_order
    #         ).sum()
    #         filter_counts[filter_order][filter_combination] = filter_count

    # This did not work before - as the combinations were not iterated through the filter order(s) first!
    for filter_order_index, filter_combinations in enumerate(
        filter_orders_combinations_criteria
    ):
        filter_counts.append({})
        site_ref_nums.append({})
        for each_filter_combination in filter_combinations:
            mask = hld_data_df_filtered[list(each_filter_combination)] == 1
            mask = mask.all(axis=1)
            hld_data_df_filtered_masked = hld_data_df_filtered[mask]

            filter_count = hld_data_df_filtered_masked.shape[0]

            # filter_count = (
            #     hld_data_df_filtered[list(each_filter_combination)].sum(axis=1)
            #     == 1
            # ).sum()

            # TODO: Fill in EAHLD Reference Numbers for each of the filter counts
            filter_counts[filter_order_index][each_filter_combination] = filter_count
            site_ref_nums[filter_order_index][
                each_filter_combination
            ] = hld_data_df_filtered_masked["HLD reference"].values.tolist()

    return filter_counts, site_ref_nums


def construct_count_statistics_dataframe(
    filter_criteria_combinations_counts_dict: dict[
        str, tuple[list[dict[str, list]], list[dict[str, list]]]
    ]
) -> pandas.DataFrame:
    """
    Construct correctly formatted output DataFrame from statistics dictionary - to be written to an Excel worksheet.
    :param filter_criteria_combinations_counts_dict:
    :return:
    """
    count_statistics_df = pandas.DataFrame()
    observation_index = 1

    # fmt: off
    site_refs = set()  # Across 12 primary filters, 10 filter orders and 10 waste criteria (total number of sites <= hld_df rows)  # noqa
    site_refs_per_primary_filter_per_filter_order = {}  # For each primary filter, for each filter order - (each of 12 subplots in a filter order tab)  # noqa
    # fmt: on

    for (
        primary_filter_name,
        (primary_filter_site_counts, primary_filter_site_ref_nums),
    ) in filter_criteria_combinations_counts_dict.items():

        # fmt: off
        site_refs_per_primary_filter_per_filter_order[primary_filter_name] = {}  # For each primary filter, for each filter order (each of 12 subplots in a filter order tab)  # noqa
        # fmt: on

        for each_filter_order, (
            each_primary_filter_site_counts,
            each_primary_filter_site_ref_nums,
        ) in enumerate(zip(primary_filter_site_counts, primary_filter_site_ref_nums)):
            site_refs_per_primary_filter_per_filter_order[primary_filter_name][
                each_filter_order
            ] = []

            temp_1_site_refs = set()
            for x in each_primary_filter_site_ref_nums.values():
                temp_1_site_refs.update(x)
            site_refs_per_primary_filter_per_filter_order[primary_filter_name][
                each_filter_order
            ].extend(list(temp_1_site_refs))
            site_refs_per_primary_filter_per_filter_order[primary_filter_name][
                each_filter_order
            ] = list(
                numpy.unique(
                    numpy.array(
                        site_refs_per_primary_filter_per_filter_order[
                            primary_filter_name
                        ][each_filter_order]
                    )
                )
            )
            site_refs.update(
                site_refs_per_primary_filter_per_filter_order[primary_filter_name][
                    each_filter_order
                ]
            )

            stats_output_dict = {
                "Primary CE property filter": str(primary_filter_name),
                "Filter Order (r Value in nCr)": each_filter_order + 1,
                "Corresponding site ref nums per primary filter per Filter Order": str(
                    site_refs_per_primary_filter_per_filter_order[primary_filter_name][
                        each_filter_order
                    ]
                ),
                "Num unique site refs per primary filter per Filter Order": len(
                    site_refs_per_primary_filter_per_filter_order[primary_filter_name][
                        each_filter_order
                    ]
                ),
                "Total num unique site refs": len(set(site_refs)),
                "Corresponding total unique site ref nums": str(list(site_refs)),
            }

            for (
                waste_filter_criteria_name,
                waste_filter_criteria_sites_count,
            ) in each_primary_filter_site_counts.items():
                # Excel requires that these values are converted to string or other fundamental data types.
                # Writing tuples to Excel does not seem to automatically convert to string - raising ValueError.

                stats_output_dict["Experiment Index"] = observation_index
                stats_output_dict["Waste Filter Criteria"] = str(
                    waste_filter_criteria_name
                )
                stats_output_dict["Num sites"] = waste_filter_criteria_sites_count
                stats_output_dict["Corresponding site ref nums"] = str(
                    each_primary_filter_site_ref_nums[waste_filter_criteria_name]
                )

                temp_row = pandas.Series(stats_output_dict)
                count_statistics_df = pandas.concat(
                    [count_statistics_df, temp_row.to_frame().T], ignore_index=True
                )
                count_statistics_df = count_statistics_df[STATS_DF_OUTPUT_COLS]
                observation_index += 1

    return count_statistics_df


def check_site_totals(hld_df: pandas.DataFrame, site_ref_nums: list[dict[str, list]]):
    """Ensure that the site totals count stays within the limits of available sites in the DataFrame (sanity check)."""
    site_totals = []
    for each_filter_order in site_ref_nums:
        site_totals_per_order = []
        for key, value in each_filter_order.items():
            site_totals_per_order.extend(value)
        site_totals.extend(site_totals_per_order)
    assert len(set(site_totals)) <= hld_df.shape[0]


def get_count_statistics_filter_criteria(
    dataset_path: pathlib.Path,
    input_data_sheet_name: str,
    output_data_sheet_name: str,
    cols: list[int],
    filter_column_name: str,
    primary_filter_criteria: list[tuple[str, str]],
    secondary_filter_criteria: list[tuple[str, str]],
    combination_operator: typing.Optional[typing.Callable],
    enable_crs_conversion: bool,
    enable_postcode_extraction: bool,
    multiprocessing_options: MultiProcessingOptionsEnum,
    intermediate_state_file_path: pathlib.Path,
    load_existing: bool,
    enable_permutation: bool,
) -> pandas.DataFrame:
    """
    Compute count statistics for various filter combinations and cache results in a new sheet in the dataset Excel file.
    :param dataset_path:
    :param input_data_sheet_name:
    :param output_data_sheet_name:
    :param cols:
    :param filter_column_name:
    :param primary_filter_criteria:
    :param secondary_filter_criteria:
    :param combination_operator:
    :param enable_crs_conversion:
    :param enable_postcode_extraction:
    :param multiprocessing_options:
    :param intermediate_state_file_path:
    :param load_existing:
    :param enable_permutation:
    :return:
    """
    # Get combinations of filters
    (
        col_indices_nums_combs,
        col_indices_chars_combs,
        col_filtration_criteria_combs,
    ) = get_filter_combinations_criteria_multiple_orders()

    # Unzip the filter tuple values into their constituent filter names and filter criteria respectively
    primary_filter_criteria_names, primary_filter_criteria_un = zip(
        *primary_filter_criteria
    )
    secondary_filter_criteria_names, secondary_filter_criteria_un = zip(
        *secondary_filter_criteria
    )
    logger.info(
        f"Calculating and saving count statistics for combinations of filters. "
        f"Note that this computation might take a while to complete!"
    )
    if enable_permutation:
        # TODO: We probably don't ever need count statistics for permutations of filters. If so, add implementation.
        logger.warning(
            f"Please note that permutations might take much longer than combinations!"
        )
        logger.warning(
            f"Usually combinations would be sufficient in order to compute the count statistics. "
            f"Please reconsider if you really need permutations. If so, please make sure you have sufficiently "
            f"available memory, processing power and other hardware resources to support this operation!"
        )
        # Get permutations of filters
        (
            col_indices_nums_perms,
            col_indices_chars_perms,
            col_filtration_criteria_perms,
        ) = get_filter_permutations_criteria_multiple_orders()

        raise NotImplementedError(
            "Permutation operations are not currently supported yet."
        )

    hld_df = load_initial_filtered_data(
        dataset_path=dataset_path,
        sheet_name=input_data_sheet_name,
        cols=cols,
        filter_column_name=filter_column_name,
        filter_criteria=primary_filter_criteria_un,
        combination_operator=combination_operator,
        enable_crs_conversion=enable_crs_conversion,
        enable_postcode_extraction=enable_postcode_extraction,
        multiprocessing_options=multiprocessing_options,
        intermediate_state_file_path=intermediate_state_file_path,
        load_existing=load_existing,
    )

    hld_df_primary_filter_criteria_data_dict = {}
    hld_df_secondary_filter_criteria_data_dict = {}
    filter_criteria_combinations_counts_dict = {}
    (
        filter_criteria_combinations_counts_on_all_hld,
        site_ref_nums_on_all_hld,
    ) = get_filter_criteria_counts(hld_df, col_filtration_criteria_combs)

    check_site_totals(hld_df, site_ref_nums_on_all_hld)

    hld_df_primary_filter_criteria_data_dict["# Sites relevant CE"] = hld_df

    filter_criteria_combinations_counts_dict["# Sites relevant CE"] = (
        filter_criteria_combinations_counts_on_all_hld,
        site_ref_nums_on_all_hld,
    )

    # Get primary filter data and their corresponding count statistics
    for filter_criteria_name, filter_criteria in primary_filter_criteria:
        temp_df = hld_df[
            hld_df[on_and_adjacent_ce_property_filter_column_name] == filter_criteria
        ]

        temp_df_counts, temp_site_ref_nums = get_filter_criteria_counts(
            temp_df, col_filtration_criteria_combs
        )
        check_site_totals(temp_df, temp_site_ref_nums)

        filter_criteria_combinations_counts_dict[filter_criteria_name] = (
            temp_df_counts,
            temp_site_ref_nums,
        )
        hld_df_primary_filter_criteria_data_dict[filter_criteria_name] = temp_df

    # Get secondary filter data and combine them with primary filter data and their corresponding count statistics.
    # The secondary filter is usually just a boolean value ["Yes", "No"]
    for filter_index in range(len(secondary_filter_criteria) + 1):
        for (
            primary_filter_name,
            primary_filtered_data,
        ) in hld_df_primary_filter_criteria_data_dict.items():

            if filter_index < len(secondary_filter_criteria):
                filter_criteria = secondary_filter_criteria_un[filter_index]
                filter_criteria_name = (
                    primary_filter_name
                    + " + "
                    + secondary_filter_criteria_names[filter_index]
                )
                temp_df = primary_filtered_data[
                    primary_filtered_data[filter_criteria] == "Yes"
                ]

            else:
                filter_criteria_name = (
                    primary_filter_name
                    + " + "
                    + secondary_filter_criteria_names[0]
                    + " + "
                    + secondary_filter_criteria_names[1]
                )
                mask = (
                    primary_filtered_data[list(secondary_filter_criteria_un)] == "Yes"
                )
                mask = mask.all(axis=1)
                temp_df = primary_filtered_data[mask]

            temp_df_counts, temp_site_ref_nums = get_filter_criteria_counts(
                temp_df, col_filtration_criteria_combs
            )
            check_site_totals(temp_df, temp_site_ref_nums)

            filter_criteria_combinations_counts_dict[filter_criteria_name] = (
                temp_df_counts,
                temp_site_ref_nums,
            )
            hld_df_secondary_filter_criteria_data_dict[filter_criteria_name] = temp_df

    count_statistics_df = construct_count_statistics_dataframe(
        filter_criteria_combinations_counts_dict
    )
    save_dataframe_to_existing_worksheet(
        count_statistics_df, dataset_path, output_data_sheet_name
    )

    return count_statistics_df


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    """
    Main entry point for reading HCL dataset, optionally computing count statistics for various filter
    combinations - if the results have not been pre-computed and cached, and plotting appropriate interactive plots
    using plotly and dash.
    """
    # TODO: Establish correct directory structure and modularise various utilities, dependencies, functions and methods.
    # TODO: Reuse various functions across the whole project - without having to copy them manually into every folder.
    # TODO: Fix circular import problems across the whole project
    # TODO: Update and Fix tests
    # TODO: Improve test pyramid (unit, component, module, integration, end2end)
    # TODO: Include releases file and update README + documentation (possible Sphynx integration?)
    # TODO: Add multiprocessing support to speed up computation by running them in parallel.

    # TODO: Remove this code-block that just checks if the counts sum up to the correct value.
    # for (
    #     filter_order_key,
    #     filter_order_values,
    # ) in filter_criteria_combs_on_and_adjacent_ce_property_counts.items():
    #     for key, value in filter_order_values.items():
    #         assert (
    #             value
    #             == filter_criteria_combs_only_on_ce_property_counts[
    #                 filter_order_key
    #             ][key]
    #             + filter_criteria_combs_only_adjacent_ce_property_counts[
    #                 filter_order_key
    #             ][key]
    #         )

    on_and_adjacent_ce_property_filter_column_name = "New Update CE Property Jan 2023?"
    USE_EXISTING_STATS: bool = True  # Usually set this to true if underlying data has not changed. Improves speed.
    # USE_EXISTING_STATS: bool = False

    RUN_DASH_SERVER: bool = True
    # RUN_DASH_SERVER: bool = False

    # ENABLE_PERMUTATION: bool = True
    ENABLE_PERMUTATION: bool = False  # Usually set this to false for 99% of cases and significantly improved speed.

    SAVE_INDIVIDUAL_PLOTS: bool = True
    # SAVE_INDIVIDUAL_PLOTS: bool = False

    ENABLE_SERVER_DEBUG_MODE: bool = True
    # ENABLE_SERVER_DEBUG_MODE: bool = False

    with MeasureTimer() as measure_timer:
        if not USE_EXISTING_STATS:
            filter_combinations_count_statistics_df = get_count_statistics_filter_criteria(
                dataset_path=QUALIFIED_DATASET_FILE,
                input_data_sheet_name="Sites",
                output_data_sheet_name="Aggregated Statistics",
                cols=USEFUL_COLUMN_NUMS,
                filter_column_name=on_and_adjacent_ce_property_filter_column_name,
                primary_filter_criteria=[
                    ("# Sites on CE", "Yes"),
                    ("# Sites adj CE", "Adjacent"),
                ],
                secondary_filter_criteria=[
                    ("Borders", "Borders other site(s)"),
                    ("Nearby", "Site(s) nearby"),
                ],
                combination_operator=operator.or_,
                enable_crs_conversion=False,
                enable_postcode_extraction=False,
                multiprocessing_options=MultiProcessingOptionsEnum.MULTI_PROCESS_INCLUDING_LOGICAL_CORES_WITH_HT,
                intermediate_state_file_path=QUALIFIED_INTERMEDIATE_PICKLE_FILE,
                load_existing=True,
                enable_permutation=ENABLE_PERMUTATION,
            )

            dash_app_instance = plot_count_statistics_filter_combinations(
                dataset_path=None,
                sheet_name=None,
                hld_df=filter_combinations_count_statistics_df,
                save_individual_plots=SAVE_INDIVIDUAL_PLOTS,
            )

        else:
            dash_app_instance = plot_count_statistics_filter_combinations(
                dataset_path=QUALIFIED_DATASET_FILE,
                sheet_name="Aggregated Statistics",
                hld_df=None,
                save_individual_plots=SAVE_INDIVIDUAL_PLOTS,
            )

    if RUN_DASH_SERVER:
        dash_app_instance.run_server(debug=ENABLE_SERVER_DEBUG_MODE)

    logger.info(
        f"All done! Programme execution took: {time.strftime('%H:%M:%S', time.gmtime(measure_timer.elapsed_time))}"
    )
