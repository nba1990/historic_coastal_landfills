# Read pre-computed HCL count statistics and plot them on interactive subplot(s).
# Concept: Plotting various interactive subplots using Plotly and Dash.
# Project: Historic Coastal Landfills
# Organisation: A Future Without Rubbish CIC, UK
# Author: Bharadwaj Raman
# Date First Authored: 07/02/2023
import pathlib
import typing

import dash
import pandas
import plotly.express
import plotly.subplots

from hcl_constants.constants import logger

SUBPLOT_ROWS = 4
SUBPLOT_COLS = 3

SUBPLOTS_HEIGHT = 1200
SUBPLOTS_WIDTH = 1900
INTERACTIVE_PLOTS_OUTPUT_PATH = pathlib.Path("saved_outputs/interactive")
EXPORTED_PLOTS_OUTPUT_PATH = pathlib.Path("saved_outputs/exported_images")


def plot_count_statistics_filter_combinations(
    dataset_path: typing.Optional[pathlib.Path],
    sheet_name: typing.Optional[str],
    hld_df: typing.Optional[pandas.DataFrame],
    save_individual_plots: bool,
):

    # If both dataset_path and hld_df are None, raise Error [0, 0, 0]
    if dataset_path is None and sheet_name is None and hld_df is None:
        raise ValueError(
            "At least one of either `dataset_path` along with the corresponding statistics `sheet_name` "
            "or statistics DataFrame `hld_df` must be provided."
        )
    # If only hld_df is provided, use it. [0, 0, 1] and [1, 0, 1] and [0, 1, 1] and [1, 1, 1]
    if hld_df is not None and (dataset_path is None or sheet_name is None):
        logger.info(
            f"Using the provided `hld_df` DataFrame and ignoring `sheet_name` and `dataset_path`..."
        )
    # If no dataset_path is provided, raise Error [0, 1, 0].
    if sheet_name is not None and dataset_path is None and hld_df is None:
        raise ValueError(
            f"Only `sheet_name`: {sheet_name} was provided but no `dataset_path` to along with it."
        )
    # If no sheet_name is provided, raise Error [1, 0, 0].
    if dataset_path is not None and sheet_name is None and hld_df is None:
        raise ValueError(
            f"Only `dataset_path`: {dataset_path} was provided but no `sheet_name` to along with it."
        )
    # If only dataset_path and sheet_name are provided, use them [1, 1, 0].
    if dataset_path is not None and sheet_name is not None and hld_df is None:
        logger.info(
            f"Using the provided `dataset_path`: {dataset_path} and `sheet_name`: {sheet_name} "
            f"and ignoring `hld_df`: {hld_df}."
        )

    if hld_df is None:
        # Load the input dataframe
        hld_df = pandas.read_excel(dataset_path, sheet_name)

    hld_df_non_zero_num_sites = hld_df[hld_df["Num sites"] > 0]

    # Create a list of unique "Filter Order (r Value in nCr)" values
    filter_order_r_values = (
        hld_df_non_zero_num_sites["Filter Order (r Value in nCr)"].unique().tolist()
    )

    # Create a list of unique "Primary CE property filter" values
    ce_property_filter_values = (
        hld_df_non_zero_num_sites["Primary CE property filter"].unique().tolist()
    )

    waste_filter_criteria_order_plots_figs = []
    filter_order_plots_figs = []

    for filter_order_index, filter_order in enumerate(filter_order_r_values):
        waste_filter_criteria_subplots_figs = []

        hld_df_per_filter_order_r = hld_df_non_zero_num_sites[
            hld_df_non_zero_num_sites["Filter Order (r Value in nCr)"] == filter_order
        ]
        hld_df_per_filter_order_r_non_zero = hld_df_per_filter_order_r[
            hld_df_per_filter_order_r["Num sites"] > 0
        ]

        # Create a bar plot for each "Primary CE property filter"
        for primary_filter in ce_property_filter_values:
            hld_df_per_ce_property_filter = hld_df_per_filter_order_r[
                hld_df_per_filter_order_r["Primary CE property filter"]
                == primary_filter
            ]
            hld_df_per_ce_property_filter_non_zero = hld_df_per_ce_property_filter[
                hld_df_per_ce_property_filter["Num sites"] > 0
            ]

            each_subplot_fig = plotly.express.bar(
                hld_df_per_ce_property_filter,
                x="Waste Filter Criteria",
                y="Num sites",
            )
            # fig.update_xaxes(fixedrange=False, rangeslider_visible=True)
            waste_filter_criteria_subplots_figs.append(each_subplot_fig)

        # update_menus = [
        #     dict(
        #         type="buttons",
        #         # showactive=False,
        #         showactive=True,
        #         buttons=[
        #             dict(
        #                 label="Collapse All",
        #                 method="update",
        #                 args=[
        #                     {
        #                         "collapsed": [
        #                             [True] * len(waste_filter_criteria_subplots_figs)
        #                         ]
        #                     }
        #                 ],
        #             ),
        #             dict(
        #                 label="Expand All",
        #                 method="update",
        #                 args=[
        #                     {
        #                         "collapsed": [
        #                             [False] * len(waste_filter_criteria_subplots_figs)
        #                         ]
        #                     }
        #                 ],
        #             ),
        #         ],
        #     )
        # ]

        subplot_titles = [
            "Filter Order = {} | {} | Unique Total: {}".format(
                filter_order,
                primary_filter,
                hld_df_per_filter_order_r[
                    hld_df_per_filter_order_r["Primary CE property filter"]
                    == primary_filter
                ]["Num unique site refs per primary filter per Filter Order"].max(),
            )
            for index, primary_filter in enumerate(ce_property_filter_values)
        ]
        filter_order_plot_fig = plotly.subplots.make_subplots(
            rows=SUBPLOT_ROWS,
            cols=SUBPLOT_COLS,
            subplot_titles=subplot_titles,
            # vertical_spacing=0.1,
            # horizontal_spacing=0.1,
            shared_yaxes="all",
            print_grid=False,
        )

        for fig_index, waste_filter_criteria_subplot_fig in enumerate(
            waste_filter_criteria_subplots_figs
        ):
            fig_row_index = fig_index // 3 + 1
            fig_col_index = fig_index % 3 + 1
            filter_order_plot_fig.update_xaxes(showticklabels=False)
            # fig.update_layout(xaxis_visible=False, xaxis_showticklabels=False)
            filter_order_plot_fig.add_trace(
                waste_filter_criteria_subplot_fig["data"][0],
                row=fig_row_index,
                col=fig_col_index,
            )

        # filter_order_plot_fig.update_layout(
        #     title=f"Interactive Plot of Number of Sites by Waste Filter Criteria - "
        #           f"Filter Combination Higher Order: {filter_order}",
        #     title_x=0.5,
        #     updatemenus=update_menus,
        # )

        filter_order_plot_fig.update_layout(
            title=f"Interactive Plot of Number of Sites by Waste Filter Criteria - "
            f"Filter Combination Higher Order: {filter_order} |  Unique Total: "
            f"{hld_df_per_filter_order_r['Total num unique site refs'].max()}",
            title_x=0.5,
            height=SUBPLOTS_HEIGHT,
            width=SUBPLOTS_WIDTH,
        )

        if save_individual_plots:
            file_name = f"num_sites_per_criteria_filter_order_{filter_order}_plots"
            interactive_plot_file_name = file_name + ".html"
            export_images_file_name = file_name + ".png"
            qualified_interactive_plots_file_name = (
                INTERACTIVE_PLOTS_OUTPUT_PATH / interactive_plot_file_name
            )
            qualified_exported_plots_file_name = (
                EXPORTED_PLOTS_OUTPUT_PATH / export_images_file_name
            )
            filter_order_plot_fig.write_html(
                qualified_interactive_plots_file_name,
                auto_open=False,
            )
            filter_order_plot_fig.write_image(
                qualified_exported_plots_file_name,
                format="png",
            )

            # TODO: Add automatic export of PNG images from HTML files as well
            logger.info(
                f"Saved interactive plots into file: {qualified_interactive_plots_file_name}"
            )
            logger.info(
                f"Saved exported image of plots into file: {qualified_exported_plots_file_name}"
            )

        waste_filter_criteria_order_plots_figs.append(
            waste_filter_criteria_subplots_figs
        )
        filter_order_plots_figs.append(filter_order_plot_fig)

    # Create a Dash app instance
    dash_app_instance = dash.Dash(__name__)

    dash_app_instance.layout = dash.html.Div(
        [
            dash.dcc.Tabs(
                id="tabs",
                children=[
                    dash.dcc.Tab(
                        label="Filter Order - " + str(each_filter_order_value),
                        value=str(each_filter_order_value),
                        children=[dash.dcc.Graph(figure=each_filter_order_plot_fig)],
                    )
                    for index, (
                        each_filter_order_value,
                        each_filter_order_plot_fig,
                    ) in enumerate(zip(filter_order_r_values, filter_order_plots_figs))
                ],
            )
        ]
    )

    return dash_app_instance
