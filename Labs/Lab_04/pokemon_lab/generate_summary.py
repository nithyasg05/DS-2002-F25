#!/usr/bin/env python3
import os
import sys
import pandas as pd

def generate_summary(portfolio_file):
    """
    Reads the portfolio CSV, computes key summary stats, and prints results.
    """

    # Verify file exists
    if not os.path.exists(portfolio_file):
        print(f"Error: Portfolio file '{portfolio_file}' not found.", file=sys.stderr)
        sys.exit(1)

    # Read portfolio CSV
    df = pd.read_csv(portfolio_file)

    # Check for empty data
    if df.empty:
        print("The portfolio file is empty. No data to summarize.")
        return

    # Calculate total portfolio value
    total_portfolio_value = df['card_market_value'].sum()

    # Find the most valuable card
    most_valuable_idx = df['card_market_value'].idxmax()
    most_valuable_card = df.loc[most_valuable_idx]

    # Print summary report
    print("\n===== Portfolio Summary Report =====")
    print(f"Total Portfolio Value: ${total_portfolio_value:,.2f}")
    print("\nMost Valuable Card:")
    print(f"  Name: {most_valuable_card['card_name']}")
    print(f"  ID: {most_valuable_card['card_id']}")
    print(f"  Market Value: ${most_valuable_card['card_market_value']:,.2f}")
    print("====================================\n")


def main():
    """Public function to summarize the main production portfolio."""
    generate_summary('card_portfolio.csv')


def test():
    """Public function to summarize the test portfolio."""
    generate_summary('test_card_portfolio.csv')


if __name__ == "__main__":
    # Default behavior: run in test mode
    test()