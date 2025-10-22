#!/usr/bin/env python3

import os
import sys
import pandas as pd
import json


# -----------------------------------------------------------
# Function 1: _load_lookup_data(lookup_dir)
# -----------------------------------------------------------

def _load_lookup_data(lookup_dir):
    all_lookup_df = []

    for file in os.listdir(lookup_dir):
        if not file.endswith(".json"):
            continue
        filepath = os.path.join(lookup_dir, file)

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                raw = f.read().strip()
            if not raw:
                continue
            data = json.loads(raw)
        except Exception:
            continue

        records = data["data"] if isinstance(data, dict) and "data" in data else data
        if not isinstance(records, list) or not records:
            continue

        df = pd.json_normalize(records, sep=".")

        # Robust market value selection
        s1 = df.get("tcgplayer.prices.holofoil.market", pd.Series(0.0, index=df.index))
        s2 = df.get("tcgplayer.prices.normal.market",   pd.Series(0.0, index=df.index))
        df["card_market_value"] = s1.fillna(s2).fillna(0.0)

        df = df.rename(
            columns={
                "id": "card_id",
                "name": "card_name",
                "number": "card_number",
                "set.id": "set_id",
                "set.name": "set_name",
            }
        )

        for col in ["card_id", "card_name", "card_number", "set_id", "set_name", "card_market_value"]:
            if col not in df.columns:
                df[col] = pd.NA if col != "card_market_value" else 0.0

        required_cols = [
            "card_id",
            "card_name",
            "card_number",
            "set_id",
            "set_name",
            "card_market_value",
        ]
        all_lookup_df.append(df[required_cols].copy())

    if not all_lookup_df:
        return pd.DataFrame(columns=[
            "card_id", "card_name", "card_number", "set_id", "set_name", "card_market_value"
        ])

    lookup_df = pd.concat(all_lookup_df, ignore_index=True)
    lookup_df = lookup_df.sort_values(by="card_market_value", ascending=False)
    lookup_df = lookup_df.drop_duplicates(subset=["card_id"], keep="first")
    return lookup_df

# -----------------------------------------------------------
# Function 2: _load_inventory_data(inventory_dir)
# -----------------------------------------------------------

def _load_inventory_data(inventory_dir):
    inventory_data = []
    for file in os.listdir(inventory_dir):
        if file.endswith(".csv"):
            filepath = os.path.join(inventory_dir, file)
            df = pd.read_csv(filepath)
            inventory_data.append(df)

    if not inventory_data:
        return pd.DataFrame()

    inventory_df = pd.concat(inventory_data, ignore_index=True)
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

    left_cols = [c for c in inventory_df.columns if c not in ("card_name", "set_name")]
    merged_df = pd.merge(
        inventory_df[left_cols],
        lookup_df[["card_id", "card_name", "set_name", "card_market_value"]],
        on="card_id",
        how="left",
        suffixes=("_inv", "")
    )

    def s(df, col, default=pd.NA):
        return df[col] if col in df.columns else pd.Series(default, index=df.index)

    merged_df["card_name"] = (
        s(merged_df, "card_name")
        .combine_first(s(merged_df, "card_name_inv"))
        .fillna("NOT_FOUND")
    )

    merged_df["set_name"] = (
        s(merged_df, "set_name")
        .combine_first(s(merged_df, "set_name_inv"))
        .fillna("NOT_FOUND")
    )

    merged_df["card_market_value"] = s(merged_df, "card_market_value", 0.0).fillna(0.0)

    merged_df["index"] = (
        merged_df["binder_name"].astype(str) + "-" +
        merged_df["page_number"].astype(str) + "-" +
        merged_df["slot_number"].astype(str)
    )

    final_cols = [
        "index", "binder_name", "page_number", "slot_number",
        "card_name", "card_number", "set_id", "set_name", "card_market_value"
    ]

    merged_df[final_cols].to_csv(output_file, index=False)
    print(f"Portfolio successfully updated: {output_file}")

# -----------------------------------------------------------
# Function 4: main() - Production mode
# -----------------------------------------------------------
def main():
    update_portfolio("./card_inventory/", "./card_set_lookup/", "card_portfolio.csv")

# -----------------------------------------------------------
# Function 5: test() - Test mode
# -----------------------------------------------------------
def test():
    update_portfolio("./card_inventory_test/", "./card_set_lookup_test/", "test_card_portfolio.csv")

# -----------------------------------------------------------
# Main Execution Block
# -----------------------------------------------------------
if __name__ == "__main__":
    print("Starting Pokémon TCG Portfolio ETL Pipeline in Test Mode...", file=sys.stderr)
    test()
