Hey Dustin, can you add two env vars to the ti prod deployment, or give me the permission to?

Environment > Environment Variables > Edit Deployment Variables:

OPTIMIZER_SLACK_CHANNEL = C0BSTH6E84T (not secret)
SLACK_BOT_TOKEN = I'll DM you the value (mark SECRET)

They let the Spark optimizer's daily digest post to #spark-optimizer instead of a GCS file nobody opens. Bot is the existing airflow-debugger app Robin approved, chat:write only, already in the channel.
