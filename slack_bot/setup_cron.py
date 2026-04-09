"""Install a macOS launchd plist to run the pipeline daily."""

import os
import subprocess
import sys
from pathlib import Path

PLIST_NAME = "com.mntn.slack-knowledge-bot"
PLIST_PATH = Path.home() / "Library" / "LaunchAgents" / f"{PLIST_NAME}.plist"
SLACK_BOT_DIR = Path(__file__).resolve().parent
LOG_DIR = SLACK_BOT_DIR / "logs"


def install():
    """Create and load the launchd plist."""
    LOG_DIR.mkdir(exist_ok=True)

    python_path = subprocess.run(
        ["which", "python3"], capture_output=True, text=True
    ).stdout.strip()

    # Get env vars needed at runtime
    slack_token = os.environ.get("SLACK_BOT_TOKEN", "")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if not slack_token:
        print("ERROR: SLACK_BOT_TOKEN not set in environment")
        sys.exit(1)
    if not anthropic_key:
        print("ERROR: ANTHROPIC_API_KEY not set in environment")
        sys.exit(1)

    plist_content = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{PLIST_NAME}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{python_path}</string>
        <string>{SLACK_BOT_DIR / "run_daily.py"}</string>
    </array>
    <key>WorkingDirectory</key>
    <string>{SLACK_BOT_DIR}</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>SLACK_BOT_TOKEN</key>
        <string>{slack_token}</string>
        <key>ANTHROPIC_API_KEY</key>
        <string>{anthropic_key}</string>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>0</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>{LOG_DIR / "stdout.log"}</string>
    <key>StandardErrorPath</key>
    <string>{LOG_DIR / "stderr.log"}</string>
    <key>RunAtLoad</key>
    <false/>
</dict>
</plist>
"""

    # Unload if already loaded
    if PLIST_PATH.exists():
        subprocess.run(["launchctl", "unload", str(PLIST_PATH)], capture_output=True)

    PLIST_PATH.write_text(plist_content)
    print(f"Wrote plist → {PLIST_PATH}")

    subprocess.run(["launchctl", "load", str(PLIST_PATH)], check=True)
    print(f"Loaded {PLIST_NAME}")
    print(f"Scheduled to run daily at 06:00 local time")
    print(f"Logs → {LOG_DIR}")
    print(f"\nTo test manually: launchctl start {PLIST_NAME}")
    print(f"To uninstall: launchctl unload {PLIST_PATH} && rm {PLIST_PATH}")


def uninstall():
    """Unload and remove the plist."""
    if PLIST_PATH.exists():
        subprocess.run(["launchctl", "unload", str(PLIST_PATH)], capture_output=True)
        PLIST_PATH.unlink()
        print(f"Removed {PLIST_PATH}")
    else:
        print("Plist not found — nothing to remove")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Manage launchd cron job")
    parser.add_argument("action", choices=["install", "uninstall"], help="Action to perform")
    args = parser.parse_args()

    if args.action == "install":
        install()
    else:
        uninstall()
