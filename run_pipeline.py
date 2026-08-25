import subprocess
import sys
import argparse
from datetime import date


def run_step(name, command, continue_on_failure=False):
    print("=" * 60)
    print(f"Running: {name}")
    print("=" * 60)
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"Step failed: {name}")
        if continue_on_failure:
            print(f"Continuing after failed optional step: {name}\n")
            return False
        exit(1)
    print(f"Completed: {name}\n")
    return True


def main():
    parser = argparse.ArgumentParser(description="Run the daily news pipeline")
    parser.add_argument("--report-date", type=date.fromisoformat, default=None)
    args = parser.parse_args()
    python = sys.executable
    report_arg = f" --report-date {args.report_date.isoformat()}" if args.report_date else ""

    run_step(
        "Fetch Google News",
        f"{python} app/fetchers/google_news.py",
        continue_on_failure=True,
    )
    run_step(
        "Fetch Yahoo Finance",
        f"{python} app/fetchers/yahoo_finance.py",
        continue_on_failure=True,
    )
    run_step(
        "Refresh Ticker Market/Earnings Metadata",
        f"{python} app/fetchers/refresh_earnings_yfinance.py",
        continue_on_failure=True,
    )
    run_step("Fetch SEC 8-K Filings", f"{python} app/fetchers/sec_edgar.py")
    run_step("Process SEC Documents", f"{python} app/processors/process_sec_documents.py --window-hours 24")
    run_step("Process Articles", f"{python} app/processors/process_articles.py")
    run_step(
        "Generate Company Summaries",
        f"{python} app/summarizers/summarize_by_company.py{report_arg}",
    )

    print("All steps completed successfully.")


if __name__ == "__main__":
    main()
