## THIS IS THE CODE THAT WILL RUN ALL THE INDIVIDUAL SCRIPT THAT WILL SEND DDP EMAIL THEN SEND SLACK NOTIFICATIONS##

import subprocess
from datetime import datetime

import pandas as pd
import requests
from google.cloud import bigquery

PROJECT_ID = 'dw-main-gold'

# Slack webhook (channel configured in Slack Workflow Builder)
SLACK_WEBHOOK_URL = (
    'https://hooks.slack.com/triggers/E08AUP9RC2G/11076390477776/'
    'a23aaff2e2c80c4919fc8ee3f5d29e5d'
)  # slack channel #liveramp_usage_billing

ddp_script_map = {
    17: 'ddpmonthlyusageemail-Sharethis.py',
    24: 'ddpmonthlyusageemail-Justuno.py',
    28: 'ddpmonthlyusageemail-33Across.py',
    29: 'ddpmonthlyusageemail-Deepsync.py',
    33: 'ddpmonthlyusageemail-Sovrn.py',
    35: 'ddpmonthlyusageemail-Liveramp.py',
    36: 'ddpmonthlyusageemail-Cybba.py',
}


def log(message, level='INFO'):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [{level}] {message}")


def format_report_month(reporting_month):
    return pd.Timestamp(reporting_month).strftime('%Y-%m-%d')


def send_slack_notification(webhook_url, report_month=None, ddp=None, status=None):
    payload = (
        {'report_month': report_month, 'ddp': ddp, 'status': status}
        if ddp is not None
        else {'status': status}
    )
    response = requests.post(webhook_url, json=payload, timeout=30)
    if not response.ok:
        raise RuntimeError(f"Webhook failed ({response.status_code}): {response.text}")
    return response


def main():
    bq_client = bigquery.Client(project=PROJECT_ID)

    audit_sql = '''
    SELECT DISTINCT
        bi.data_source_id,
        ds.name AS ddp_name,
        bi.reporting_month,
        CASE
            WHEN bi.override_status IS NOT NULL THEN bi.override_status
            ELSE bi.final
        END AS status
    FROM `dw-main-bronze.coredw.usage_reporting_audits` bi
    LEFT JOIN `dw-main-bronze.integrationprod.data_sources` ds
        ON ds.data_source_id = bi.data_source_id
    WHERE bi.reporting_month = DATE_TRUNC(
        DATE_SUB(CURRENT_DATE('America/Los_Angeles'), INTERVAL 1 MONTH),
        MONTH
    )
    '''
    audit_df = bq_client.query(audit_sql).to_dataframe()

    status_lookup = {
        int(row.data_source_id): {
            'ddp_name': row.ddp_name,
            'reporting_month': row.reporting_month,
            'status': str(row.status).lower(),
        }
        for row in audit_df.itertuples(index=False)
    }

    all_complete = all(
        status_lookup.get(data_source_id, {}).get('status') == 'pass'
        for data_source_id in ddp_script_map
    )

    if all_complete:
        log('✅ All DDPs passed and completed. Sending success emails and Slack notifications.')
        for ddp_id, script_name in ddp_script_map.items():
            reporting_month = status_lookup[ddp_id]['reporting_month']
            ddp_name = status_lookup[ddp_id]['ddp_name']

            result = subprocess.run(['python', script_name, str(ddp_id)])

            email_status = 'Email sent' if result.returncode == 0 else 'Email failed'

            try:
                send_slack_notification(
                    SLACK_WEBHOOK_URL,
                    report_month=format_report_month(reporting_month),
                    ddp=ddp_name,
                    status=email_status,
                )
                log(f"✅ Slack notification sent for DSID {ddp_name} with status '{email_status}'")
            except Exception as e:
                log(f"❌ Slack notification failed for DSID {ddp_name}: {e}", level='ERROR')
    else:
        log('❌ Not all DDPs passed. Sending failure Slack notification.')
        failure_text = 'Not all DDPs are complete for the previous reporting month.'
        try:
            send_slack_notification(SLACK_WEBHOOK_URL, status=failure_text)
            log('✅ Failure Slack notification sent.')
        except Exception as e:
            log(f"❌ Failure Slack notification failed: {e}", level='ERROR')


if __name__ == '__main__':
    main()
