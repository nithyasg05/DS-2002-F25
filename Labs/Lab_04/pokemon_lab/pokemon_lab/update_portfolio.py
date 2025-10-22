#!/usr/bin/env python3
"""
update_portfolio.py
This script performs the full ETL pipeline for Pokémon card data:
- Extracts data from the TCG API lookup JSONs and local inventory CSVs.
- Transforms and merges them to calculate card market values.
- Loads the final output as a portfolio CSV.
"""

import os
import sys
import pandas as pd


# -----------------------------------------------------------
# Function 1: _load_lookup_data(lookup_dir)
# -----------------------------------------------------------
def _load_lookup_data(lookup_dir):
    """Extract and transform JSON lookup data into a unified dataframe."""
    all_lookup_df = []

    for file in os.listdir(lookup_dir):
        if file.endswith(".json"):
            filepath = os.path.join(lookup_dir, file)
            with open(filepath, "r", encoding="utf-8") as f:
                data = pd.read_json(f)

            # Flatten JSON using the 'data' key
            df = pd.json_normalize(data["data"])

            # Create the market value column (prioritize holofoil > normal > 0)
            df["card_market_value"] = (
                df["tcgplayer.prices.holofoil.market"]
                .fillna(df["tcgplayer.prices.normal.market"])
                .fillna(0.0)
            )

            # Rename columns for readability
            df = df.rename(
                columns={
                    "id": "card_id",
                    "name": "card_name",
                    "number": "card_number",
                    "set.id": "set_id",
                    "set.name": "set_name",
                }
            )

            # Select relevant columns
            required_cols = [
                "card_id",
                "card_name",
                "card_number",
                "set_id",
                "set_name",
                "card_market_value",
            ]
            all_lookup_df.append(df[required_cols].copy())

    # Combine all lookup data into one dataframe
    if not all_lookup_df:
        return pd.DataFrame(
            columns=[
                "card_id",
                "card_name",
                "card_number",
                "set_id",
                "set_name",
                "card_market_value",
            ]
        )

    lookup_df = pd.concat(all_lookup_df, ignore_index=True)

    # Remove duplicates (keep highest market value)
    lookup_df = lookup_df.sort_values("card_market_value", ascending=False)
    lookup_df = lookup_df.drop_duplicates(subset=["card_id"], keep="first")

    return lookup_df


# -----------------------------------------------------------
# Function 2: _load_inventory_data(inventory_dir)
# -----------------------------------------------------------
def _load_inventory_data(inventory_dir):
    """Extract and transform inventory CSV data into a unified dataframe."""
    inventory_data = []

    for file in os.listdir(inventory_dir):
        if file.endswith(".csv"):
            filepath = os.path.join(inventory_dir, file)
            df = pd.read_csv(filepath)
            inventory_data.append(df)

    # Handle empty inventory
    if not inventory_data:
        return pd.DataFrame(
            columns=[
                "card_name",
                "set_id",
                "card_number",
                "binder_name",
                "page_number",
                "slot_number",
                "card_id",
            ]
        )

    # Combine all inventory data
    inventory_df = pd.concat(inventory_data, ignore_index=True)

    # Create a unified card_id for merging
    inventory_df["card_id"] = (
        inventory_df["set_id"].astype(str) + "-" + inventory_df["card_number"].astype(str)
    )

    return inventory_df


# -----------------------------------------------------------
# Function 3: update_portfolio(inventory_dir, lookup_dir, output_file)
# -----------------------------------------------------------
def update_portfolio(inventory_dir, lookup_dir, output_file):
    """Main ETL function that merges lookup and inventory data into final portfolio."""
    lookup_df = _load_lookup_data(lookup_dir)
    inventory_df = _load_inventory_data(inventory_dir)

    # Handle empty inventory case
    if inventory_df.empty:
        sys.stderr.write("Error: Inventory is empty. Creating empty portfolio.\n")
        empty_cols = [
            "index",
            "card_name",
            "set_name",
            "card_market_value",
            "binder_name",
            "page_number",
            "slot_number",
        ]
        pd.DataFrame(columns=empty_cols).to_csv(output_file, index=False)
        return

    # Merge on card_id
    portfolio_df = pd.merge(
        inventory_df,
        lookup_df[
            ["card_id", "card_name", "set_name", "card_market_value"]
        ],
        on="card_id",
        how="left",
    )

    # Fill missing values
    portfolio_df["card_market_value"] = portfolio_df["card_market_value"].fillna(0.0)
    portfolio_df["set_name"] = portfolio_df["set_name"].fillna("NOT_FOUND")

    # Create index for binder location
    portfolio_df["index"] = (
        portfolio_df["binder_name"].astype(str)
        + "-"
        + portfolio_df["page_number"].astype(str)
        + "-"
        + portfolio_df["slot_number"].astype(str)
    )

    # Final column order
    final_cols = [
        "index",
        "card_name",
        "set_name",
        "card_market_value",
        "binder_name",
        "page_number",
        "slot_number",
    ]

    # Write to output file
    portfolio_df.to_csv(output_file, index=False)
    print(f"✅ Portfolio successfully updated and saved to {output_file}")


# -----------------------------------------------------------
# Function 4: main() - Production mode
# -----------------------------------------------------------
def main():
    """Run ETL in production mode."""
    update_portfolio(
        inventory_dir="./card_inventory/",
        lookup_dir="./card_set_lookup/",
        output_file="card_portfolio.csv",
    )


# -----------------------------------------------------------
# Function 5: test() - Test mode
# -----------------------------------------------------------
def test():
    """Run ETL in test mode."""
    update_portfolio(
        inventory_dir="./card_inventory_test/",
        lookup_dir="./card_set_lookup_test/",
        output_file="test_card_portfolio.csv",
    )


# -----------------------------------------------------------
# Main Execution Block
# -----------------------------------------------------------
if __name__ == "__main__":
    sys.stderr.write("⚙️  Running update_portfolio.py in Test Mode...\n")
    test()

