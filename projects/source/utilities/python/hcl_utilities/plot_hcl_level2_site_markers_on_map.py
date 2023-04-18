# Read Level 2 dataset
# Enrich data by cross-referencing with HCL canonical dataset, preprocess data, plot site markers on an interactive map.
# Concept: Plotting HCL Level2 coordinate markers on a base Folium Map with interactive popups and layers.
# Project: Historic Coastal Landfills
# Organisation: A Future Without Rubbish CIC, UK
# Author: Bharadwaj Raman
# Date First Authored: 07/03/2023
import pathlib
import time
import typing

import folium
import pandas

from hcl_constants.constants import CURRENT_DIR, PARENT_DATASET_PATH
from hcl_constants.constants import USEFUL_COLS as HLD_USEFUL_COLS
from hcl_constants.constants import MultiProcessingOptionsEnum, logger
from plot_hcl_site_markers_on_map import (
    create_initial_folium_map,
    plot_site_markers_on_map,
)
from preprocess.initial_preprocess import reorder_df_columns
from preprocess.stages_preprocess import run_first_stage
from read_io.excel_io import (
    ExcelLoadWBContextManagerSupported,
    convert_useful_col_names_to_col_letters_and_indices,
    load_excel_column_headers,
    read_dataset_to_df,
)
from timing.timer import MeasureTimer
from write_io.interim_state_pickle import (
    read_intermediate_state,
    save_intermediate_state,
)

DATASET_FILE_NAME = "UK_Historic_Landfill_Norfolk_Level_2_Sites.xlsx"
QUALIFIED_DATASET_FILE = PARENT_DATASET_PATH / DATASET_FILE_NAME
QUALIFIED_HLD_DATASET_FILE = PARENT_DATASET_PATH / "UK_Historic_Landfill_Sites.xlsx"
HLD_INTERMEDIATE_PICKLE_FILE_NAME = pathlib.Path(
    "saved_intermediate_states/hld_df_enriched_whole_dataset.pkl"
)
INTERMEDIATE_PICKLE_FILE_NAME = "saved_intermediate_states/hld_df_norfolk_level2.pkl"
QUALIFIED_INTERMEDIATE_PICKLE_FILE = CURRENT_DIR / INTERMEDIATE_PICKLE_FILE_NAME
FOLIUM_MAP_FILE_NAME = "saved_outputs/norfolk_level2_map.html"
QUALIFIED_FOLIUM_MAP_FILE = CURRENT_DIR / FOLIUM_MAP_FILE_NAME

USEFUL_COLS = [
    "Index",
    "HLD reference",
    "Site name",
    "Site address",
    "Borders other site(s)",
    "Site(s) nearby",
    "200m From CE Property (Now)",
]

USEFUL_COLS_SELECTION = [
    "200m From CE Property (Now)",
    "Site County",
    "Country",
    "Borders other site(s)",
    "Site(s) nearby",
]


def run_second_stage(
    hld_df: pandas.DataFrame,
    filter_column_name: str,
) -> folium.Map:
    """Run the second stage of the pipeline by plotting HCL site markers on an instance of OpenStreetMap."""
    logger.info("Running second stage of the pipeline.")

    hld_df_relevant = hld_df[
        (hld_df[filter_column_name] == "Yes") | (hld_df[filter_column_name] == "No")
    ]
    folium_map = create_initial_folium_map(hld_df_relevant[["Latitude", "Longitude"]])
    hld_df_on_ce_property = hld_df[hld_df[filter_column_name] == "Yes"]
    hld_df_not_on_ce_property = hld_df[hld_df[filter_column_name] == "No"]
    logger.info(
        f"Number of sites - {filter_column_name}: {hld_df_on_ce_property.shape[0]} | "
        f"Number of sites - NOT {filter_column_name}: {hld_df_not_on_ce_property.shape[0]} | "
        f"Total: {hld_df_relevant.shape[0]}"
    )

    assert (
        hld_df_on_ce_property.shape[0] + hld_df_not_on_ce_property.shape[0]
        == hld_df_relevant.shape[0]
    )

    # Plot site markers for sites that are on CE property
    folium_map = plot_site_markers_on_map(
        hld_df=hld_df_on_ce_property,
        marker_colour="red",
        marker_layer_name=f"{filter_column_name}",
        folium_map=folium_map,
    )

    # Plot site markers for sites that are adjacent to CE property
    folium_map = plot_site_markers_on_map(
        hld_df=hld_df_not_on_ce_property,
        marker_colour="blue",
        marker_layer_name=f"NOT {filter_column_name}",
        folium_map=folium_map,
    )

    # Add the layer control
    folium.LayerControl().add_to(folium_map)

    logger.info("Finished second stage...")
    return folium_map


def get_row_number_for_hld_ref(worksheet, hld_reference_value: str) -> int:
    """Get the corresponding Excel Worksheet row number for a given HLD reference value."""
    logger.info(
        f"Getting corresponding row number in the Excel Worksheet for the HLD_reference: {hld_reference_value}"
    )
    for row in worksheet.iter_rows(min_row=2, min_col=2, max_row=worksheet.max_row):
        if row[0].value == hld_reference_value:
            row_index = row[0].row
            logger.info(
                f"Found row number: {row_index} for HLD_reference: {hld_reference_value}"
            )
            return row_index
    return -1


def overwrite_cells_in_excel_worksheet(
    qualified_file_name: pathlib.Path, data_df: pandas.DataFrame
):
    """Overwrite only relevant cells in an Excel worksheet document (in place) with values from pandas DataFrame."""
    # Load the column headers
    col_headers, col_letters, col_indices = load_excel_column_headers(
        qualified_file_name
    )

    with ExcelLoadWBContextManagerSupported(
        qualified_file_name, read_only=False
    ) as workbook:
        # Select the worksheet you want to modify
        worksheet = workbook["Sites"]

        # Convert the column headers to column letters and indices
        (
            column_letters,
            column_indices,
        ) = convert_useful_col_names_to_col_letters_and_indices(
            USEFUL_COLS_SELECTION, col_headers, col_letters, col_indices
        )

        # Iterate through each row of the dataframe
        for index, row in data_df.iterrows():

            # Find the corresponding row number in the Excel worksheet
            hld_reference_value = row["HLD reference"]
            hld_reference_row_number = get_row_number_for_hld_ref(
                worksheet, hld_reference_value
            )

            # Iterate through each column of the dataframe
            for column_name, column_index, column_letter in zip(
                USEFUL_COLS_SELECTION, column_indices, column_letters
            ):
                # Get the cell value from the pandas DataFrame
                cell_value = row[column_name]

                if hld_reference_row_number > 0:
                    # Get the cell coordinates
                    cell_coordinates = f"{column_letter}{hld_reference_row_number}"

                    old_cell_value = worksheet[cell_coordinates].value

                    if old_cell_value != cell_value:
                        logger.info(
                            f"Overwriting cell: {cell_coordinates} containing original value: "
                            f"<{old_cell_value}> | with the new value: <{cell_value}>"
                        )
                    # Overwrite the cell value in the Excel worksheet
                    worksheet[cell_coordinates] = cell_value

        # Save the modified Excel worksheet
        workbook.save(qualified_file_name)
        logger.info(f"Saved the modified Excel worksheet: {qualified_file_name}")


def enrich_hld_dataset_with_level2_details(
    full_hld_df: pandas.DataFrame, level2_df: pandas.DataFrame
):
    """Enrich canonical HCL dataset with details from level 2 analysis."""
    # Get the HLD references present in both dataframes
    shared_hld_refs = set(full_hld_df["HLD reference"]).intersection(
        set(level2_df["HLD reference"])
    )

    level2_df["Site County"] = "Norfolk"
    level2_df["Country"] = "England"

    # Select only the relevant columns from level2_df
    level2_subset_df = level2_df[["HLD reference"] + USEFUL_COLS_SELECTION]

    # Merge the full_hld_df with the level2_subset_df on "HLD reference"
    merged_df = pandas.merge(
        full_hld_df, level2_subset_df, on="HLD reference", how="left"
    )

    # Overwrite the respective column in full_hld_df with the values from level2_df
    for col_name in USEFUL_COLS_SELECTION:
        merged_df[col_name] = merged_df[f"{col_name}_y"].fillna(
            merged_df[f"{col_name}_x"]
        )
        merged_df.drop([f"{col_name}_x", f"{col_name}_y"], axis=1, inplace=True)

    merged_df = reorder_df_columns(merged_df)
    merged_df_relevant = merged_df[
        (merged_df["200m From CE Property (Now)"] == "Yes")
        | (merged_df["200m From CE Property (Now)"] == "No")
    ]
    logger.info(
        f"Overwriting corresponding cells (in place) in the Excel Worksheet: {QUALIFIED_HLD_DATASET_FILE} "
        f"with the level 2 analysis values..."
    )
    overwrite_cells_in_excel_worksheet(QUALIFIED_HLD_DATASET_FILE, merged_df_relevant)

    return merged_df


def run_programme(
    dataset_path: pathlib.Path,
    sheet_name: str,
    sheet_index: int,
    filter_column_name: typing.Optional[str],
    filter_criteria: typing.Optional[list[str]],
    combination_operator: typing.Optional[typing.Callable],
    enable_postcode_extraction: bool,
    multiprocessing_options: MultiProcessingOptionsEnum,
    intermediate_state_file_path: pathlib.Path,
    output_map_file_path: pathlib.Path,
    load_existing: bool = True,
) -> tuple[list[int], pandas.DataFrame]:
    """
    Main entry point for the whole programme to plot HCL site markers on the map from the dataset.

    Supported arguments/parameters and their examples:
    dataset_path: pathlib.Path = QUALIFIED_DATASET_FILE
    input_data_sheet_name: str = "Sites"
    sheet_index: int = 0
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
    markers_scope: SiteMarkersScopeEnum = [SiteMarkersScopeEnum.ONLY_CE_PROPERTIES, SiteMarkersScopeEnum.ALL_HCL_SITES]
    output_map_file_path: pathlib.Path = QUALIFIED_FOLIUM_MAP_FILE
    load_existing: bool = [True, False]
    """
    column_headers, column_letters, column_indices = load_excel_column_headers(
        dataset_path=dataset_path, sheet_index=sheet_index
    )
    (
        useful_column_letters,
        useful_column_nums,
    ) = convert_useful_col_names_to_col_letters_and_indices(
        USEFUL_COLS, column_headers, column_letters, column_indices
    )

    if load_existing:
        if not HLD_INTERMEDIATE_PICKLE_FILE_NAME.exists():
            hld_df = run_first_stage(
                QUALIFIED_HLD_DATASET_FILE,
                "Sites",
                HLD_USEFUL_COLS,
                filter_column_name,
                filter_criteria,
                combination_operator,
                enable_postcode_extraction,
                multiprocessing_options,
                HLD_INTERMEDIATE_PICKLE_FILE_NAME,
            )
        else:
            hld_df = read_intermediate_state(
                file_path=HLD_INTERMEDIATE_PICKLE_FILE_NAME
            )

        if not intermediate_state_file_path.exists():
            hld_df_norfolk_level_2 = read_dataset_to_df(
                dataset_path=dataset_path,
                sheet_name=sheet_name,
                cols=useful_column_nums,
                useful_cols=USEFUL_COLS,
            )

            # Replace and standardise values to fit the pattern in the main Excel HCL HLD dataset.
            hld_df_norfolk_level_2.replace(["Y", "True", True], "Yes", inplace=True)
            hld_df_norfolk_level_2.replace(["N", "False", False], "No", inplace=True)

            save_intermediate_state(
                hld_df_norfolk_level_2, intermediate_state_file_path
            )

        else:
            hld_df_norfolk_level_2 = read_intermediate_state(
                file_path=intermediate_state_file_path
            )

    else:  # Run the first stage regardless of pre-existing intermediate state
        hld_df = run_first_stage(
            QUALIFIED_HLD_DATASET_FILE,
            "Sites",
            HLD_USEFUL_COLS,
            filter_column_name,
            filter_criteria,
            combination_operator,
            enable_postcode_extraction,
            multiprocessing_options,
            HLD_INTERMEDIATE_PICKLE_FILE_NAME,
        )

        # TODO: Remove later
        # hld_column_headers, hld_column_letters, hld_column_indices = load_excel_column_headers(
        #     dataset_path=QUALIFIED_HLD_DATASET_FILE, sheet_index=sheet_index
        # )
        #
        # (
        #     hld_useful_column_letters,
        #     hld_useful_column_nums,
        # ) = convert_useful_col_names_to_col_letters_and_indices(
        #     HLD_USEFUL_COLS, hld_column_headers, hld_column_letters, hld_column_indices
        # )
        #
        # hld_df = read_dataset_to_df(dataset_path=QUALIFIED_HLD_DATASET_FILE, sheet_name="Sites",
        #                             cols=hld_useful_column_nums, useful_cols=HLD_USEFUL_COLS)

        hld_df_norfolk_level_2 = read_dataset_to_df(
            dataset_path=dataset_path,
            sheet_name=sheet_name,
            cols=useful_column_nums,
            useful_cols=USEFUL_COLS,
        )

        # Replace and standardise values to fit the pattern in the main Excel HCL HLD dataset only in the last 3 cols.
        hld_df_norfolk_level_2.replace(["Y", "Y ", "True", True], "Yes", inplace=True)
        hld_df_norfolk_level_2.replace(["N", "N ", "False", False], "No", inplace=True)

        save_intermediate_state(hld_df_norfolk_level_2, intermediate_state_file_path)
    logger.info(f"Enriching dataset with level 2 dataset analysis")
    hld_df = enrich_hld_dataset_with_level2_details(hld_df, hld_df_norfolk_level_2)

    folium_map = run_second_stage(hld_df, filter_column_name)

    logger.info(f"Saving the final map to: {output_map_file_path}")
    folium_map.save(f"{output_map_file_path}")

    return useful_column_nums, hld_df


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    # TODO: Support command line arguments for running the programme through terminal based applications.
    # TODO: Add support for persistence and resume operations + error handling and recovery.
    # TODO: Improve project-wide docstrings and update params and args support.
    """Main entry point for reading HCL dataset and plotting appropriate site markers on an interactive map."""

    # Refer: https://github.com/psf/black/issues/451
    # Refer: https://stackoverflow.com/a/58584557

    with MeasureTimer() as measure_timer:
        # useful_cols_nums, hld_df_dataset = run_programme(
        #     dataset_path=QUALIFIED_DATASET_FILE,
        #     input_data_sheet_name="Sheet1",
        #     sheet_index=0,
        #     filter_column_name=None,
        #     primary_filter_criteria=None,
        #     combination_operator=None,
        #     enable_postcode_extraction=False,
        #     multiprocessing_options=MultiProcessingOptionsEnum.MULTI_PROCESS_INCLUDING_LOGICAL_CORES_WITH_HT,
        #     intermediate_state_file_path=QUALIFIED_INTERMEDIATE_PICKLE_FILE,
        #     markers_scope=SiteMarkersScopeEnum.ONLY_CE_PROPERTIES,
        #     output_map_file_path=QUALIFIED_FOLIUM_MAP_FILE,
        #     load_existing=False,
        # )  # Usually takes 00:02:33 minutes (24x slower)

        useful_cols_nums, hld_df_dataset = run_programme(
            dataset_path=QUALIFIED_DATASET_FILE,
            sheet_name="Sheet1",
            sheet_index=0,
            filter_column_name="200m From CE Property (Now)",
            filter_criteria=None,
            combination_operator=None,
            enable_postcode_extraction=False,
            multiprocessing_options=MultiProcessingOptionsEnum.MULTI_PROCESS_INCLUDING_LOGICAL_CORES_WITH_HT,
            intermediate_state_file_path=QUALIFIED_INTERMEDIATE_PICKLE_FILE,
            output_map_file_path=QUALIFIED_FOLIUM_MAP_FILE,
            load_existing=True,
        )  # Usually takes 00:00:02 seconds

    logger.info(
        f"All done! Programme execution took: {time.strftime('%H:%M:%S', time.gmtime(measure_timer.elapsed_time))}"
    )
