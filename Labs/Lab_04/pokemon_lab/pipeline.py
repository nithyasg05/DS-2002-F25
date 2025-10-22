#!/usr/bin/env python3
import sys
import update_portfolio
import generate_summary

def run_production_pipeline():
    """
    Runs the full production workflow:
    1. Executes the ETL step (update_portfolio.main)
    2. Executes the reporting step (generate_summary.main)
    """

    print("--- Starting Production Pipeline ---", file=sys.stderr)

    # ETL Step
    print(">>> Step 1: Updating portfolio data...", file=sys.stderr)
    update_portfolio.main()

    # Reporting Step
    print(">>> Step 2: Generating summary report...", file=sys.stderr)
    generate_summary.main()

    print("--- Production Pipeline Completed Successfully ---", file=sys.stderr)


if __name__ == "__main__":
    run_production_pipeline()
