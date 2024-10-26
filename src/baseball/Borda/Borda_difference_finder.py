import pandas as pd

def process_borda_method(method, output, top_n):
    input_file = f"./src/baseball/Borda/borda-comparison-top{top_n}.csv"
    data = pd.read_csv(input_file)

    # Select relevant columns for processing
    # Assuming the next column after the method is its corresponding ranks
    method_col = method
    ranks_col = f'Ranks-{method.split("-")[-1]}'  # Create the ranks column name based on the method

    differences = data[['Season', 'League', 'Official Borda', method_col, ranks_col]].copy()

    # Filter out rows where the rank is either 1 or ranks that are 1, 2, ..., up to top_n
    def is_valid_ranking(ranks):
        rank_list = list(map(int, ranks.split(','))) if isinstance(ranks, str) else []
        # Check if the first rank is 1 and all ranks are in consecutive order up to the length of the rank_list
        return not (rank_list == list(range(1, len(rank_list) + 1)) and all(r <= top_n for r in rank_list))

    # Apply filtering to differences DataFrame
    differences = differences[differences[ranks_col].apply(is_valid_ranking)]

    # Save the filtered results to the output CSV file
    differences.to_csv(output, index=False, header=True)


def generate_borda_methods(top_n):
    # List of Borda methods to include
    methods = ['Borda-Top1', 'Borda-Top3', 'Borda-Top5', 'Borda-Top10', 'Dowdall']
    borda_methods = []

    for method in methods:
        # Create the output file path based on the method name and top_n
        output_file = f'./src/baseball/Borda/difference/top{top_n}-{method.lower()}-differences.csv'
        borda_methods.append((method, output_file))

    return borda_methods


top_n = 5
borda_methods_n = generate_borda_methods(top_n)


for method, output in borda_methods_n:
    process_borda_method(method, output, top_n)
