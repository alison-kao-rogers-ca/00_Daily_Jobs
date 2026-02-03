# -*- coding: utf-8 -*-
"""
Created on Thu Dec 18 15:20:19 2025

@author: Alison.Kao
"""
# --- Imports ---
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
from datetime import datetime

# --- Build SQLAlchemy connection string ---
from urllib.parse import quote_plus

PG_DB   = "rb_prod"
PG_USER = "rb_service_prd"
PG_PWD  = "Y[~VbY$w/E2sR+@H" # see note below if it contains special chars
PG_HOST = "52.228.62.199"     # or a valid DNS name
PG_PORT = "5432"

PG_PWD_SAFE = quote_plus(PG_PWD)
conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PWD_SAFE}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Add connect args for timeout
engine = create_engine(
    conn_str,
    connect_args={"connect_timeout": 5}
)

# Quick probe: open/close a connection
try:
    with engine.connect() as con:
        print("Connected OK")
except Exception as e:
    print("Connect failed:", e)

query = """
SELECT
 B.BT_DESC AS EPP_DESC
,LPAD(
   A.ACCT_ID::TEXT
  ,11
  ,'0')
   AS ACCT_ID
,A.ACCT_SEQ_NUM
,A.TXN_POST_DT AS EPP_POSTED_DT
,A.TXN_DT AS EPP_TXN_DT
,A.TXN_EXP_DT AS EPP_EXPIRY_DT
,A.TXN_AMT AS EPP_TXN_AMT
,A.PMT_RTE AS MONTHLY_EPP_PYMT_AMT
FROM
 ADSCA_DW_ACC.ACCT_TXN_PROC_CURR A
,DATA_ANALYTICS.BT_TLP_CD B
WHERE
 A.TXN_LVL_PROC_OPTION_SET_ID = B.TLP_OPTION_SET::TEXT AND
 A.REASGNED_TXN_CAT_CD = B.TCAT_CD::TEXT  AND
 B.TCAT_CD IN (2010
              ,2011) AND
 A.TXN_POST_DT >= DATE_TRUNC('MONTH', NOW() - INTERVAL '1 DAY') AND
 A.TXN_POST_DT <  DATE_TRUNC('DAY', NOW())
ORDER BY
 A.TXN_POST_DT
,A.TXN_LVL_PROC_OPTION_SET_ID
,A.ACCT_ID;
"""
 # --- Execute query into DataFrame using SQLAlchemy engine ---
df = pd.read_sql(query, engine)


 # --- Build output path safely ---
current_date = datetime.today()
report_date_str = current_date.strftime('%Y%m')

output_dir = Path(r"C:\Users\Alison.Kao\Automation\Output")
output_dir.mkdir(parents=True, exist_ok=True)

output_path = output_dir / f"EPP_POSTED_{report_date_str}.xlsx"
df.to_excel(output_path, index=False, engine="openpyxl")

# -- Send out email
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from os.path import basename
import smtplib

# ========== Configuration (EDIT THESE) ==========
# Output folder where your files were saved
dest_folder = Path(r"C:\Users\Alison.Kao\Automation\Output")

# Sender and recipients
sender_email = "rbdataanalytics@rci.rogers.com"
recipients = [
    "alison.kao@rci.rogers.com",
    "Justin.Xie@rci.rogers.com",
    "Alyssa.Bui@rci.rogers.com"
]

# Subject + SharePoint link in the body
subject = "09_EPP_POSTED"
sharepoint_url = "https://bank.rogers.com/teams/DataAnalytics/03_Custom_Reports/05_Operations/Forms/AllItems.aspx?RootFolder=%2Fteams%2FDataAnalytics%2F03%5FCustom%5FReports%2F05%5FOperations%2FEPP%5FRELATED&FolderCTID=0x012000356E2B2EEFE2BE4AB3ADFFD65C9A096C&View=%7BEE0C7869%2DE15F%2D44A5%2D94F3%2D6AFC5E2C0225%7D"  # <-- replace with your actual URL

# SMTP server settings (confirm with IT)
smtp_server = "smtp.rci.rogers.com"
smtp_port = 25  # if your org requires TLS/auth, switch to 587 + starttls/login

# Attach only these explicit filenames (recommended), or set to None to attach all .xlsx
expected_files = [
   f"EPP_POSTED_{report_date_str}.xlsx"
]
DELETE_AFTER_SEND = True  # set to False if you want to keep the files
# ===============================================


# --- Resolve attachments ---
if expected_files and len(expected_files) > 0:
    attachment_paths = []
    missing = []
    for name in expected_files:
        p = dest_folder / name
        if p.exists():
            attachment_paths.append(p)
        else:
            missing.append(name)
    if missing:
        raise FileNotFoundError(
            f"The following expected files were not found in {dest_folder}:\n" +
            "\n".join(missing)
        )
else:
    # Attach all .xlsx in the folder
    attachment_paths = sorted(dest_folder.glob("*.xlsx"))
    if not attachment_paths:
        raise FileNotFoundError(f"No .xlsx files found in {dest_folder}")

print("Will attach:")
for p in attachment_paths:
    print(f" - {p.name}")

# --- Build the email (minimal HTML body with SharePoint link) ---
body_html = f"""
<html>
  <body>
    <p>
      Files are attached.<br>
      You can also access them here:
      <a href="{sharepoint_url}" target="_blank">Open SharePoint folder</a>
    </p>
  </body>
</html>
"""

message = MIMEMultipart()
message["From"] = sender_email
message["To"] = ", ".join(recipients)
message["Subject"] = subject
message.attach(MIMEText(body_html, "html"))

# Attach files
for p in attachment_paths:
    with open(p, "rb") as fp:
        part = MIMEApplication(fp.read(), Name=basename(p))
    part['Content-Disposition'] = f'attachment; filename="{basename(p)}"'
    message.attach(part)

# --- Send via SMTP ---
try:
    server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
    server.ehlo()

    # If your org requires TLS/auth, uncomment and configure:
    # import ssl
    # context = ssl.create_default_context()
    # server.starttls(context=context)
    # server.ehlo()
    # server.login(sender_email, "APP_PASSWORD_OR_OAUTH")

    server.sendmail(sender_email, recipients, message.as_string())
    server.quit()
    print(f"Email sent to {', '.join(recipients)} with {len(attachment_paths)} attachment(s).")

    # --- Post-send cleanup (delete attached files) ---
    if DELETE_AFTER_SEND:
        for p in attachment_paths:
            try:
                p.unlink()  # deletes the file
                print(f"Deleted file: {p}")
            except Exception as e:
                print(f"[WARN] Could not delete {p}: {e}")

except Exception as e:
    print(f"[ERROR] Sending email failed: {e}")

