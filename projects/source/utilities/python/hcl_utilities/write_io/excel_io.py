# Functions, methods and utilities for writing dataframes Excel worksheets composed into workbooks/documents.
# Concept: Excel datasets I/O related functions.
# Project: Historic Coastal Landfills
# Organisation: A Future Without Rubbish CIC, UK
# Author: Bharadwaj Raman
# Date First Authored: 04/02/2023

import pathlib

import openpyxl
import openpyxl.utils.dataframe
import pandas

from hcl_constants.constants import logger
from read_io.excel_io import ExcelLoadWBContextManagerSupported


def save_dataframe_to_existing_worksheet(
    hld_df: pandas.DataFrame, dataset_path: pathlib.Path, sheet_name: str
):
    """
    Save and write the contents of the provided pandas dataframe into a pre-existing worksheet in an Excel workbook.
    This method is idempotent and will always return the same output provided that the underlying data remain unchanged.
    :param hld_df:
    :param dataset_path:
    :param sheet_name:
    :return:
    """
    logger.info(f"Opening workbook: {dataset_path}.")
    # Load the workbook
    with ExcelLoadWBContextManagerSupported(dataset_path) as workbook:
        # Select the worksheet
        worksheet = workbook[sheet_name]

        logger.info(
            f"Clearing and overwriting existing cells in the worksheet: {sheet_name}..."
        )
        # # Clear the contents of the worksheet
        worksheet.delete_cols(1, 50)
        worksheet.delete_rows(1, 50000)

        # for each_worksheet_row in worksheet.iter_rows(values_only=True):
        #     for each_worksheet_cell in each_worksheet_row:
        #         worksheet.cell(
        #             row=each_worksheet_cell.row,
        #             column=each_worksheet_cell.col_idx,
        #             value=None,
        #         )

        logger.info(f"Saving dataframe contents to the worksheet: {sheet_name}...")

        # Write the dataframe to the worksheet
        for each_df_row in openpyxl.utils.dataframe.dataframe_to_rows(
            hld_df, index=False, header=True
        ):
            worksheet.append(each_df_row)
        # Save the workbook
        workbook.save(dataset_path)

    logger.info(
        f"Saved output dataframe to worksheet: {sheet_name} | into the workbook: {dataset_path}."
    )
