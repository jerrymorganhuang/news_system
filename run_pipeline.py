import subprocess
import sys


def run_step(name, command):
    print("=" * 60)
    print(f"Running: {name}")
    print("=" * 60)
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        print(f"Step failed: {name}")
        exit(1)
    print(f"Completed: {name}\n")


def main():
    python = sys.executable

    run_step("Fetch Google News", f"{python} app/fetchers/google_news.py")
    run_step("Process Articles", f"{python} app/processors/process_articles.py")
    run_step(
        "Generate Company Summaries",
        f"{python} app/summarizers/summarize_by_company.py",
    )

    print("All steps completed successfully.")


if __name__ == "__main__":
    main()