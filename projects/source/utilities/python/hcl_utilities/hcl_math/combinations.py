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
from hcl_math.preprocess.stages_preprocess import run_first_stage
from read_io.excel_io import (
    convert_excel_column_index_to_column_letters,
    convert_excel_column_letters_to_column_index,
    convert_useful_col_names_to_col_letters_and_indices,
    load_excel_column_headers,
)
from timing.timer import MeasureTimer
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
    EXCEL_COL_HEADERS, EXCEL_COL_LETTERS, EXCEL_COL_INDICES
)


def get_filter_combinations_criteria_multiple_orders():
    """Combine filter criteria of multiple orders using nCr - where order does not matter."""
    column_indices_num_combs = []
    column_indices_char_combs = []
    filtration_criteria_combs = []
    combined_filters_lens = []
    print(
        f"\n$$$$$$$$$$$$$$$$$$$$ COMBINATIONS FOR CRITERIA: {len(WASTE_FILTRATION_CRITERIA)} $$$$$$$$$$$$$$$$$$$$\n"
    )
    for indx in range(1, len(WASTE_FILTRATION_CRITERIA) + 1):
        print(f"------ Combining filter criteria with order r: {indx} ------")
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
        print(f"###### Number of combined filter criteria: {combined_filters_len}\n")

    print(
        f"###### Total Number of combined filter criteria: {sum(combined_filters_lens)}\n"
    )
    return (
        column_indices_num_combs,
        column_indices_char_combs,
        filtration_criteria_combs,
    )


def get_filter_permutations_criteria_multiple_orders():
    """Permute filter criteria of multiple orders using nPr - where order matters."""
    column_indices_num_perms = []
    column_indices_char_perms = []
    filtration_criteria_perms = []
    permuted_filters_lens = []
    print(
        f"\n$$$$$$$$$$$$$$$$$$$$ PERMUTATIONS FOR CRITERIA: {len(WASTE_FILTRATION_CRITERIA)}  $$$$$$$$$$$$$$$$$$$$\n"
    )
    for indx in range(1, len(WASTE_FILTRATION_CRITERIA) + 1):
        print(f"------ Permuting filter criteria with order r: {indx} ------")
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
        print(f"###### Number of permuted filter criteria: {permuted_filters_len}\n")

    print(
        f"###### Total Number of permuted filter criteria: {sum(permuted_filters_lens)}\n"
    )
    return (
        column_indices_num_perms,
        column_indices_char_perms,
        filtration_criteria_perms,
    )


def get_filter_criteria_counts(hld_data_df: pandas.DataFrame):
    filter_counts = {}
    # One Hot Encode to 0 and 1 for easier comparison and filtering
    hld_data_df_filtered = hld_data_df[WASTE_FILTRATION_CRITERIA].eq("Yes").astype(int)

    for filter_order in range(1, len(WASTE_FILTRATION_CRITERIA) + 1):
        filter_counts[filter_order] = {}
        for filter_combination in itertools.combinations(
            WASTE_FILTRATION_CRITERIA, filter_order
        ):
            filter_count = (
                hld_data_df_filtered[list(filter_combination)].sum(axis=1)
                == filter_order
            ).sum()
            filter_counts[filter_order][filter_combination] = filter_count

    return filter_counts


def get_initial_filtered_data(
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
):
    """

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


if __name__ == "__main__":
    # TODO: Establish correct directory structure and modularise various utilities, dependencies, functions and methods.
    # TODO: Reuse various functions across the whole project - without having to copy them manually into every folder.
    # TODO: Fix circular import problems across the whole project
    # TODO: Add functionality to calculate statistics for "Borders other site(s)" and "Site(s) nearby" in combination and addition to the existing statistics  # noqa

    # TODO: Secondary filters:
    #  ["On CE Property", "Adjacent CE Property", "Borders other site(s)", "Site(s) nearby",
    #  "On CE Property + Borders other site(s)", "On CE Property + Site(s) nearby",
    #  "Adjacent CE Property + Borders other site(s)", "Adjacent CE Property + Site(s) nearby",
    #  "On CE Property + Borders other site(s) + Site(s) nearby",
    #  "Adjacent CE Property + Borders other site(s) + Site(s) nearby"]
    # # Get combinations of filters
    # (
    #     col_indices_nums_combs,
    #     col_indices_chars_combs,
    #     col_filtration_criteria_combs,
    # ) = get_filter_combinations_criteria_multiple_orders()

    # # Get permutations of filters
    # (
    #     col_indices_nums_perms,
    #     col_indices_chars_perms,
    #     col_filtration_criteria_perms,
    # ) = get_permutations_multiple_orders()

    on_and_adjacent_ce_property_filter_column_name = "New Update CE Property Jan 2023?"
    with MeasureTimer() as measure_timer:
        hld_df = get_initial_filtered_data(
            dataset_path=QUALIFIED_DATASET_FILE,
            sheet_name="Sites",
            cols=USEFUL_COLUMN_NUMS,
            filter_column_name=on_and_adjacent_ce_property_filter_column_name,
            filter_criteria=["Yes", "Adjacent"],
            combination_operator=operator.or_,
            enable_crs_conversion=False,
            enable_postcode_extraction=False,
            multiprocessing_options=MultiProcessingOptionsEnum.MULTI_PROCESS_INCLUDING_LOGICAL_CORES_WITH_HT,
            intermediate_state_file_path=QUALIFIED_INTERMEDIATE_PICKLE_FILE,
            load_existing=True,
        )

        hld_df_only_on_ce_property = hld_df[hld_df[on_and_adjacent_ce_property_filter_column_name] == "Yes"]
        hld_df_only_adjacent_ce_property = hld_df[
            hld_df[on_and_adjacent_ce_property_filter_column_name] == "Adjacent"
            ]

        filter_criteria_combs_on_and_adjacent_ce_property_counts = (
            get_filter_criteria_counts(hld_df)
        )
        filter_criteria_combs_only_on_ce_property_counts = get_filter_criteria_counts(
            hld_df_only_on_ce_property
        )
        filter_criteria_combs_only_adjacent_ce_property_counts = (
            get_filter_criteria_counts(hld_df_only_adjacent_ce_property)
        )

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

    logger.info(
        f"All done! Programme execution took: {time.strftime('%H:%M:%S', time.gmtime(measure_timer.elapsed_time))}"
    )

    # Program output
    # $$$$$$$$$$$$$$$$$$$$ COMBINATIONS $$$$$$$$$$$$$$$$$$$$
    #
    # ------ Combining filters with order r: 1 ------
    # ###### Number of combined filters: 10
    #
    # ------ Combining filters with order r: 2 ------
    # ###### Number of combined filters: 45
    #
    # ------ Combining filters with order r: 3 ------
    # ###### Number of combined filters: 120
    #
    # ------ Combining filters with order r: 4 ------
    # ###### Number of combined filters: 210
    #
    # ------ Combining filters with order r: 5 ------
    # ###### Number of combined filters: 252
    #
    # ------ Combining filters with order r: 6 ------
    # ###### Number of combined filters: 210
    #
    # ------ Combining filters with order r: 7 ------
    # ###### Number of combined filters: 120
    #
    # ------ Combining filters with order r: 8 ------
    # ###### Number of combined filters: 45
    #
    # ------ Combining filters with order r: 9 ------
    # ###### Number of combined filters: 10
    #
    # ------ Combining filters with order r: 10 ------
    # ###### Number of combined filters: 1
    #
    # ###### Total Number of combined filters: 1023
    #
    #
    # $$$$$$$$$$$$$$$$$$$$ PERMUTATIONS $$$$$$$$$$$$$$$$$$$$
    #
    # ------ Permuting filters with order r: 1 ------
    # ###### Number of permuted filters: 10
    #
    # ------ Permuting filters with order r: 2 ------
    # ###### Number of permuted filters: 90
    #
    # ------ Permuting filters with order r: 3 ------
    # ###### Number of permuted filters: 720
    #
    # ------ Permuting filters with order r: 4 ------
    # ###### Number of permuted filters: 5040
    #
    # ------ Permuting filters with order r: 5 ------
    # ###### Number of permuted filters: 30240
    #
    # ------ Permuting filters with order r: 6 ------
    # ###### Number of permuted filters: 151200
    #
    # ------ Permuting filters with order r: 7 ------
    # ###### Number of permuted filters: 604800
    #
    # ------ Permuting filters with order r: 8 ------
    # ###### Number of permuted filters: 1814400
    #
    # ------ Permuting filters with order r: 9 ------
    # ###### Number of permuted filters: 3628800
    #
    # ------ Permuting filters with order r: 10 ------
    # ###### Number of permuted filters: 3628800
    #
    # ###### Total Number of permuted filters: 9864100

    # print(convert_excel_column_letters_to_column_index("I"))
    # print(convert_excel_column_index_to_column_letters(35))
