# Demo python file for explaining maths concepts easily
# Concept: Permutations nPr and Combinations nCr problem
# Project: Historic Coastal Landfills
# Organisation: A Future Without Rubbish CIC, UK
# Author: Bharadwaj Raman
# Date First Authored: 27/01/2023

# Import modules
import itertools

# Declare global variables
waste_filtration_criteria = {
    "AI": "Inert Waste",  # Column AI
    "AJ": "Industrial Waste",  # Column AJ
    "AK": "Commercial Waste",  # Column AK
    "AL": "Household Waste",  # Column AL
    "AM": "Special / hazardous Waste",  # Column AM
    "AN": "Liquid / sludge Waste",  # Column AN
    "AO": "Waste unknown",  # Column AO
    "AP": "Gas control",  # Column AP
    "AQ": "Leachate containment",  # Column AQ
    "AU": "Buffer point",  # Column AU
}


# Answer inspired by: https://stackoverflow.com/a/45312360
def convert_excel_column_index_to_column_letters(n):
    """Convert column index to Excel-style column letters, e.g., 1 = A, 26 = Z, 27 = AA, 703 = AAA."""
    name = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        name = chr(r + ord("A")) + name
    return name


# Answer inspired by: https://stackoverflow.com/a/45312360
def convert_excel_column_letters_to_column_index(name):
    """Convert column letter to Excel-style index, e.g., A = 1, Z = 26, AA = 27, AAA = 703."""
    n = 0
    for c in name:
        n = n * 26 + 1 + ord(c) - ord("A")
    return n


column_indices_num = [
    convert_excel_column_letters_to_column_index(x) - 1
    for x in waste_filtration_criteria.keys()
]

column_indices_char = list(waste_filtration_criteria.keys())


def get_combinations_multiple_orders():
    """Combine filters of multiple orders using nCr - where order does not matter."""
    column_indices_num_combs = []
    column_indices_char_combs = []
    filtration_criteria_combs = []
    combined_filters_lens = []
    print(f"\n$$$$$$$$$$$$$$$$$$$$ COMBINATIONS $$$$$$$$$$$$$$$$$$$$\n")
    for indx in range(1, len(waste_filtration_criteria) + 1):
        print(f"------ Combining filters with order r: {indx} ------")
        column_indices_num_comb = list(itertools.combinations(column_indices_num, indx))
        column_indices_num_combs.append(column_indices_num_comb)

        column_indices_char_comb = list(
            itertools.combinations(column_indices_char, indx)
        )
        column_indices_char_combs.append(column_indices_char_comb)

        filtration_criteria_comb = list(
            itertools.combinations(list(waste_filtration_criteria.values()), indx)
        )
        filtration_criteria_combs.append(filtration_criteria_comb)

        combined_filters_len = len(column_indices_num_combs[indx - 1])
        combined_filters_lens.append(combined_filters_len)
        print(f"###### Number of combined filters: {combined_filters_len}\n")

    print(f"###### Total Number of combined filters: {sum(combined_filters_lens)}\n")
    return (
        column_indices_num_combs,
        column_indices_char_combs,
        filtration_criteria_combs,
    )


def get_permutations_multiple_orders():
    """Permute filters of multiple orders using nPr - where order matters."""
    column_indices_num_perms = []
    column_indices_char_perms = []
    filtration_criteria_perms = []
    permuted_filters_lens = []
    print(f"\n$$$$$$$$$$$$$$$$$$$$ PERMUTATIONS $$$$$$$$$$$$$$$$$$$$\n")
    for indx in range(1, len(waste_filtration_criteria) + 1):
        print(f"------ Permuting filters with order r: {indx} ------")
        column_indices_num_perm = list(itertools.permutations(column_indices_num, indx))
        column_indices_num_perms.append(column_indices_num_perm)

        column_indices_char_perm = list(
            itertools.permutations(column_indices_char, indx)
        )
        column_indices_char_perms.append(column_indices_char_perm)

        filtration_criteria_perm = list(
            itertools.permutations(list(waste_filtration_criteria.values()), indx)
        )
        filtration_criteria_perms.append(filtration_criteria_perm)

        permuted_filters_len = len(column_indices_num_perms[indx - 1])
        permuted_filters_lens.append(permuted_filters_len)
        print(f"###### Number of permuted filters: {permuted_filters_len}\n")

    print(f"###### Total Number of permuted filters: {sum(permuted_filters_lens)}\n")
    return (
        column_indices_num_perms,
        column_indices_char_perms,
        filtration_criteria_perms,
    )


if __name__ == "__main__":
    # Get combinations of filters
    (
        col_indices_nums_combs,
        col_indices_chars_combs,
        col_filtration_criteria_combs,
    ) = get_combinations_multiple_orders()

    # # Get permutations of filters
    # (
    #     col_indices_nums_perms,
    #     col_indices_chars_perms,
    #     col_filtration_criteria_perms,
    # ) = get_permutations_multiple_orders()

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

    print(convert_excel_column_letters_to_column_index("I"))
    print(convert_excel_column_index_to_column_letters(35))
