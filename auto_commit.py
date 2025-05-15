
import os
import subprocess
import sys


def run_command(command):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error executing command '{command}': {result.stderr}")
        sys.exit(result.returncode)
    else:
        print(f"Command '{command}' executed successfully: {result.stdout}")


def check_remote():
    result = subprocess.run("git remote -v", shell=True, capture_output=True, text=True)
    if result.returncode != 0 or not result.stdout:
        print("No remote repository configured. Adding remote repository.")
        remote_url = "https://github.com/FrenchFriedCrypto/CryptoModel"
        run_command(f"git remote add origin {remote_url}")


def get_current_branch():
    result = subprocess.run("git branch --show-current", shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error determining current branch: {result.stderr}")
        sys.exit(result.returncode)
    branch_name = result.stdout.strip()
    if not branch_name:
        print("No branch found. Please make sure you have at least one branch in your repository.")
        sys.exit(1)
    return branch_name


def main():
    # Change to the directory of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # Check if this is a Git repository
    if not os.path.exists(os.path.join(script_dir, ".git")):
        print("This is not a Git repository. Please initialize a repository first.")
        sys.exit(1)

    # Check if remote repository is configured
    check_remote()

    # Get the current branch name
    branch = get_current_branch()
    print(f"Current branch: {branch}")

    # Pull the latest changes from the remote repository with --allow-unrelated-histories
    run_command(f"git pull origin {branch} --allow-unrelated-histories")

    # Prompt for commit message
    commit_message = input("Enter commit message (or press Enter for default 'Auto-commit'): ").strip()
    if not commit_message:
        commit_message = "Auto-commit"

    # Git commands
    commands = [
        "git add .",
        f'git commit -m "{commit_message}"',
        f"git push origin {branch}"
    ]

    # Execute commands
    for command in commands:
        run_command(command)


if __name__ == "__main__":
    main()