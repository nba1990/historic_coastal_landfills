# Read HCL canonical dataset, preprocess data, plot site markers on an interactive map.
# Concept: Plotting coordinate markers on a base Folium Map with interactive popups and layers.
# Project: Historic Coastal Landfills
# Organisation: A Future Without Rubbish CIC, UK
# Author: Bharadwaj Raman
# Date First Authored: 27/01/2023

import math
import operator
import pathlib
import time
import typing

import folium
import numpy
import pandas

from hcl_constants.constants import (
    QUALIFIED_DATASET_FILE,
    QUALIFIED_FOLIUM_MAP_FILE,
    QUALIFIED_INTERMEDIATE_PICKLE_FILE,
    MultiProcessingOptionsEnum,
    SiteMarkersScopeEnum,
    logger,
)
from preprocess.stages_preprocess import run_first_stage
from read_io.excel_io import (
    convert_useful_col_names_to_col_letters_and_indices,
    load_excel_column_headers,
)
from timing.timer import MeasureTimer
from write_io.interim_state_pickle import read_intermediate_state


def create_initial_folium_map(
    coordinates: pandas.DataFrame, tiles: str = "OpenStreetMap"
) -> folium.Map:
    """Create a base Folium map centered around the mean of the given coordinates."""
    latitude_mean = coordinates["Latitude"].mean()
    longitude_mean = coordinates["Longitude"].mean()
    logger.info(
        f"Creating the initial Folium map with tile: {tiles} centred around: {latitude_mean, longitude_mean}"
    )
    # Supported Tiles: ["OpenStreetMap", "Stamen Toner", "Stamen Terrain", "MapQuest Open Aerial", "Mapbox Bright",
    # "Mapbox Control Room", "Stamen Watercolor", "CartoDB", "CartoDB Dark Matter"]
    # Reference: https://deparkes.co.uk/2016/06/10/folium-map-tiles/
    # Reference: https://www.python-graph-gallery.com/288-map-background-with-folium
    # Reference: https://levelup.gitconnected.com/creating-interactive-maps-with-python-folium-and-some-html-f8ac716966f
    # Comparison of different tile sets: https://leaflet-extras.github.io/leaflet-providers/preview/
    folium_map = folium.Map(
        location=[latitude_mean, longitude_mean], tiles=tiles, zoom_start=5
    )

    return folium_map


def populate_each_html_table_row_popup(site_details: pandas.Series) -> str:
    """Populate a custom formatted HTML table for each site based on their details."""
    # Inspired by: https://towardsdatascience.com/folium-map-how-to-create-a-table-style-pop-up-with-html-code-76903706b88a  # noqa

    html = """
        <!DOCTYPE html>
        <html>
        <style>
        table, th, td {
          border:1px solid black;
        }
        tr:hover {background-color: cornsilk;}
        </style>
        <body>

        <h4>Landfill Site Details</h2>

        <table style="width:100%">
          <tr>
            <th style="background-color:#D6EEEE">Site Attribute Name</th>
            <th style="background-color:#D6EEEE">Site Attribute Value</th>
          </tr>
          """

    each_table_details = []
    for each_col_name, each_col_value in zip(list(site_details.index), site_details):
        if type(each_col_value) in [
            int,
            float,
            numpy.int64,
            numpy.float64,
        ] and math.isnan(each_col_value):
            each_col_value = "NA"

        tmp_str = f"""
          <tr>
            <td style="color:blue;">{each_col_name}</td>
            <td>{each_col_value}</td>
          </tr>
        """
        each_table_details.append(tmp_str)
        html += tmp_str

    html_body_rest = """
    </table>

    <p>Site details were imported from the official dataset.</p>

    </body>
    </html>
    """
    html += html_body_rest

    return html


def plot_site_markers_on_map(
    hld_df: pandas.DataFrame,
    marker_colour: str,
    marker_layer_name: str,
    folium_map: folium.Map,
) -> folium.Map:
    """Plot various site markers an already created instance of Folium Map."""

    logger.info(
        f"Plotting {hld_df.shape[0]} site markers for: {marker_layer_name} | with the colour: {marker_colour} | on the map."
    )
    # Define the marker icon style
    icon_style = "fa-solid fa-xmark"

    marker_layer = folium.FeatureGroup(name=marker_layer_name)

    # Add markers for each of the given coordinates
    for site_index, site_details in hld_df.iterrows():
        styled_icon = folium.Icon(prefix="fa", icon=icon_style, color=marker_colour)
        # Create a custom formatted HTML table for each site marker and its popup
        site_html = populate_each_html_table_row_popup(site_details)
        site_popup = folium.Popup(
            folium.Html(site_html, script=True), max_width=1200, max_height=600
        )

        # For scrollbars, try this: # Reference: https://stackoverflow.com/a/73143192
        # Make sure your iFrame width matches the max width of the folium popup
        # iframe = branca.element.IFrame(html=site_html, width=710, height=300)
        # site_popup = folium.Popup(iframe, max_width=710, max_height=650)

        folium.Marker(
            location=(site_details["Latitude"], site_details["Longitude"]),
            icon=styled_icon,
            tooltip=site_details["Site name"],
            popup=site_popup,
        ).add_to(marker_layer)

    marker_layer.add_to(folium_map)

    return folium_map


def run_second_stage(
    hld_df: pandas.DataFrame,
    filter_column_name: str,
    markers_scope: SiteMarkersScopeEnum,
) -> folium.Map:
    """Run the second stage of the pipeline by plotting HCL site markers on an instance of OpenStreetMap."""
    logger.info("Running second stage of the pipeline.")

    folium_map = create_initial_folium_map(hld_df[["Latitude", "Longitude"]])
    hld_df_on_ce_property = hld_df[hld_df[filter_column_name] == "Yes"]
    hld_df_adjacent_ce_property = hld_df[hld_df[filter_column_name] == "Adjacent"]
    hld_df_rest = hld_df[
        (hld_df[filter_column_name] != "Yes")
        & (hld_df[filter_column_name] != "Adjacent")
    ]
    logger.info(
        f"Number of sites - On CE property: {hld_df_on_ce_property.shape[0]} | "
        f"Adjacent to CE property: {hld_df_adjacent_ce_property.shape[0]} | "
        f"Currently unrelated to CE property: {hld_df_rest.shape[0]}"
    )

    assert (
        hld_df_on_ce_property.shape[0]
        + hld_df_adjacent_ce_property.shape[0]
        + hld_df_rest.shape[0]
        == hld_df.shape[0]
    )

    # Plot site markers for sites that are on CE property
    folium_map = plot_site_markers_on_map(
        hld_df=hld_df_on_ce_property,
        marker_colour="red",
        marker_layer_name="On CE Property",
        folium_map=folium_map,
    )

    # Plot site markers for sites that are adjacent to CE property
    folium_map = plot_site_markers_on_map(
        hld_df=hld_df_adjacent_ce_property,
        marker_colour="blue",
        marker_layer_name="Adjacent to CE Property",
        folium_map=folium_map,
    )

    if markers_scope == SiteMarkersScopeEnum.ALL_HCL_SITES:
        # Plot site markers for the rest of the sites currently unrelated to CE properties
        folium_map = plot_site_markers_on_map(
            hld_df=hld_df_rest,
            marker_colour="purple",
            marker_layer_name="Currently unrelated to CE Properties",
            folium_map=folium_map,
        )

    # Add the layer control
    folium.LayerControl().add_to(folium_map)

    logger.info("Finished second stage...")
    return folium_map


def run_programme(
    dataset_path: pathlib.Path,
    sheet_name: str,
    sheet_index: int,
    filter_column_name: str,
    filter_criteria: list[str],
    combination_operator: typing.Optional[typing.Callable],
    enable_postcode_extraction: bool,
    multiprocessing_options: MultiProcessingOptionsEnum,
    intermediate_state_file_path: pathlib.Path,
    markers_scope: SiteMarkersScopeEnum,
    output_map_file_path: pathlib.Path,
    load_existing: bool = True,
) -> tuple[list[int], pandas.DataFrame]:
    """
    Main entry point for the whole programme to plot HCL site markers on the map from the dataset.

    Supported arguments/parameters and their examples:
    dataset_path: pathlib.Path = QUALIFIED_DATASET_FILE
    sheet_name: str = "Sites"
    sheet_index: int = 0
    filter_column_name: str = "New Update CE Property Jan 2023?"
    filter_criteria: list[str] = ["Yes", "Adjacent"]
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
        column_headers, column_letters, column_indices
    )

    if load_existing:
        if not intermediate_state_file_path.exists():
            hld_df_filtered_enriched_reordered = run_first_stage(
                dataset_path,
                sheet_name,
                useful_column_nums,
                filter_column_name,
                filter_criteria,
                combination_operator,
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
            useful_column_nums,
            filter_column_name,
            filter_criteria,
            combination_operator,
            enable_postcode_extraction,
            multiprocessing_options,
            intermediate_state_file_path,
        )

    folium_map = run_second_stage(
        hld_df_filtered_enriched_reordered, filter_column_name, markers_scope
    )

    logger.info(f"Saving the final map to: {output_map_file_path}")
    folium_map.save(f"{output_map_file_path}")

    return useful_column_nums, hld_df_filtered_enriched_reordered


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
        #     sheet_name="Sites",
        #     sheet_index=0,
        #     filter_column_name="New Update CE Property Jan 2023?",
        #     filter_criteria=["Yes", "Adjacent"],
        #     combination_operator=operator.or_,
        #     enable_postcode_extraction=False,
        #     multiprocessing_options=MultiProcessingOptionsEnum.MULTI_PROCESS_INCLUDING_LOGICAL_CORES_WITH_HT,
        #     intermediate_state_file_path=QUALIFIED_INTERMEDIATE_PICKLE_FILE,
        #     markers_scope=SiteMarkersScopeEnum.ONLY_CE_PROPERTIES,
        #     output_map_file_path=QUALIFIED_FOLIUM_MAP_FILE,
        #     load_existing=False,
        # )  # Usually takes 00:02:33 minutes (24x slower)

        useful_cols_nums, hld_df_dataset = run_programme(
            dataset_path=QUALIFIED_DATASET_FILE,
            sheet_name="Sites",
            sheet_index=0,
            filter_column_name="New Update CE Property Jan 2023?",
            filter_criteria=["Yes", "Adjacent"],
            combination_operator=operator.or_,
            enable_postcode_extraction=False,
            multiprocessing_options=MultiProcessingOptionsEnum.MULTI_PROCESS_INCLUDING_LOGICAL_CORES_WITH_HT,
            intermediate_state_file_path=QUALIFIED_INTERMEDIATE_PICKLE_FILE,
            markers_scope=SiteMarkersScopeEnum.ONLY_CE_PROPERTIES,
            output_map_file_path=QUALIFIED_FOLIUM_MAP_FILE,
            load_existing=True,
        )  # Usually takes 00:00:02 seconds

    logger.info(
        f"All done! Programme execution took: {time.strftime('%H:%M:%S', time.gmtime(measure_timer.elapsed_time))}"
    )
