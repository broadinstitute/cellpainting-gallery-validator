"""
Script to update well and plate metadata
"""
import argparse
import pandas as pd


def upsert(curr: pd.DataFrame, new: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """
    Update a DataFrame with new data using keys as join columns
    """
    if (curr.columns != new.columns).any():
        raise ValueError("Columns must be identical")
    org_cols = curr.columns
    diff = set(keys) - set(org_cols)
    if diff:
        raise ValueError(f"{diff} columns are missing in the DataFrames")
    curr = curr.set_index(keys)
    new = new.set_index(keys)
    new = new.combine_first(curr).reset_index()
    new = new[org_cols]
    return new


def main():
    """Parse input params"""
    parser = argparse.ArgumentParser(
        description=("Update metadata with new values"),
    )
    parser.add_argument("cur_meta", help="path to the current metadata file.")
    parser.add_argument("new_meta", help="path to the new metadata file.")
    parser.add_argument("output_path", help="path to save the merged file.")
    parser.add_argument(
        "keys", nargs="+", metavar="KEY", help="key(s) to use in the join."
    )
    args = parser.parse_args()
    curr = pd.read_csv(args.cur_meta)
    new = pd.read_csv(args.new_meta)
    upsert(curr, new, args.keys).to_csv(args.output_path, index=False)
    print("Upsert completed.")


if __name__ == "__main__":
    main()
