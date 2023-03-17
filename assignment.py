import argparse
import pandas as pd
from multiprocessing import Pool

# Define command line arguments
parser = argparse.ArgumentParser(description='Lookup student records')
parser.add_argument('-n', '--name', type=str,
                    help='Substring to match in name')
parser.add_argument('-d', '--dob', type=int, help='Date of birth')
parser.add_argument('-m', '--mobile', type=int, help='Mobile number')
parser.add_argument('-c', '--city', type=str, help='City')

# Define function to load CSV files into a Pandas DataFrame


def load_data(filename):
    return pd.read_csv(filename)

# Define function to perform the student lookup


def lookup_student(args):
    data, lookup_criteria = args
    for col, val in lookup_criteria.items():
        if col == "name":
            data = data[data[col].str.contains(val, case=False)]
        else:
            data = data.loc[data[col] == val]
    return data


if __name__ == '__main__':
    # Parse command line arguments
    args = parser.parse_args()

    # Load CSV files into a Pandas DataFrame using multiprocessing
    with Pool() as p:
        # data = pd.concat(p.map(load_data, ['file1.csv', 'file2.csv', 'file3.csv', 'file4.csv', 'file5.csv', 'file6.csv', 'file7.csv', 'file8.csv', 'file9.csv', 'file10.csv']))
        data = pd.concat(
            p.map(load_data, ['sample1.csv', 'sample2.csv', 'sample3.csv']))

    # Create an index for the DataFrame
    data = data.reset_index(drop=True)

    # Define lookup criteria as a dictionary
    lookup_criteria = {}
    if args.name:
        lookup_criteria['name'] = args.name
    if args.dob:
        lookup_criteria['dob'] = args.dob
    if args.mobile:
        lookup_criteria['mobile'] = args.mobile
    if args.city:
        lookup_criteria['city'] = args.city

    # Filter the DataFrame based on the lookup criteria using multiprocessing
    with Pool() as p:
        filtered_data = pd.concat(
            p.map(lookup_student, [(data, lookup_criteria)] * 4))
    filtered_data = filtered_data.drop_duplicates(keep="first")

    print(filtered_data)
