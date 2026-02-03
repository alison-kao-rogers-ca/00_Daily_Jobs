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
 LPAD(
   ACCT_ID::TEXT
  ,11
  ,'0')
   AS ACCT_ID
,ACCT_SEQ_NUM
,TXN_POSTED_DT
,TXN_DT
,LPAD(
   OUTPT_TXN_INTRNL_CD::TEXT
  ,4
  ,'0')
   AS TRANS_CD
,CASE
   WHEN ACCTG_FUNC_CD = 'DBT' THEN 'DEBIT'
   WHEN ACCTG_FUNC_CD = 'CRT' THEN 'CREDIT'
   WHEN ACCTG_FUNC_CD = 'DRV' THEN 'DEBIT REVERSAL'
   WHEN ACCTG_FUNC_CD = 'CRV' THEN 'CREDIT REVERSAL'
   WHEN ACCTG_FUNC_CD = 'DAJ' THEN 'DEBIT ADJUSTMENT'
   WHEN ACCTG_FUNC_CD = 'CAJ' THEN 'CREDIT ADJUSTMENT'
   WHEN ACCTG_FUNC_CD = 'DAR' THEN 'DEBIT ADJUSTMENT REVERSAL'
   WHEN ACCTG_FUNC_CD = 'CAR' THEN 'CREDIT ADJUSTMENT REVERSAL'
   WHEN ACCTG_FUNC_CD = 'PFC' THEN 'PURCHASE FINANCE CHARGE DEBIT'
   WHEN ACCTG_FUNC_CD = 'CFC' THEN 'CASH FINANCE CHARGE DEBIT'
   WHEN ACCTG_FUNC_CD = 'FDA' THEN 'FINANCE CHARGE DEBIT ADJUSTMENT'
   WHEN ACCTG_FUNC_CD = 'FCA' THEN 'FINANCE CHARGE CREDIT ADJUSTMENT'
   WHEN ACCTG_FUNC_CD = 'FDV' THEN 'FINANCE CHARGE DEBIT ADJUSTMENT REVERSAL'
   WHEN ACCTG_FUNC_CD = 'FCV' THEN 'FINANCE CHARGE CREDIT ADJUSTMENT REVERSAL'
   WHEN ACCTG_FUNC_CD = 'ASD' THEN 'AUTO SMALL BALANCE WRITE OFF DEBIT'
   WHEN ACCTG_FUNC_CD = 'ASC' THEN 'AUTO SMALL BALANCE WRITE OFF CREDIT'
   WHEN ACCTG_FUNC_CD = 'MSD' THEN 'MANUAL AUTO SMALL BALANCE WRITE OFF DEBIT'
   WHEN ACCTG_FUNC_CD = 'MSC' THEN 'MANUAL AUTO SMALL BALANCE WRITE OFF CREDIT'
   WHEN ACCTG_FUNC_CD = 'RDB' THEN 'REBATE DEBIT'
   WHEN ACCTG_FUNC_CD = 'RCR' THEN 'REBATE CREDIT'
   WHEN ACCTG_FUNC_CD = 'CBR' THEN 'CREDIT BALANCE REFUND DEBIT'
   WHEN ACCTG_FUNC_CD = 'PAY' THEN 'PAYMENT'
   WHEN ACCTG_FUNC_CD = 'PRV' THEN 'PAYMENT REVERSAL'
   WHEN ACCTG_FUNC_CD = 'PRF' THEN 'PAYMENT REVERSAL WITH RETURNED CHECK FEE'
   WHEN ACCTG_FUNC_CD = 'PRN' THEN 'PAYMENT REVERSAL - NO RETURNED CHECK FEE'
   WHEN ACCTG_FUNC_CD = 'CPF' THEN 'PURCHASE FINANCE CHARGE CREDIT'
   WHEN ACCTG_FUNC_CD = 'CCF' THEN 'CASH FINANCE CHARGE CREDIT'
   WHEN ACCTG_FUNC_CD = 'SCR' THEN 'CREDIT - SYSTEM GENERATED'
   WHEN ACCTG_FUNC_CD = 'WPF' THEN 'AUTO WRITE OFF PURCHASE FINANCE CHARGE'
   WHEN ACCTG_FUNC_CD = 'WCF' THEN 'AUTO WRITE OFF CASH FINANCE CHARGE'
   WHEN ACCTG_FUNC_CD = 'CBD' THEN 'CREDIT BACKDATED'
   ELSE 'ERROR'
 END
   AS ACCTG_FUNC
,MERCH_NM
,CASE WHEN DB_CR_CD = 'C' THEN TXN_AMT ELSE TXN_AMT END TXN_AMT
FROM
 ADSCA_DW_ACC.TXN_MSTR_CURR
WHERE
 TXN_SRC_CD = 'BO' AND
 ACCTG_FUNC_CD <> 'PAY'
ORDER BY
 ACCT_ID
,ACCT_SEQ_NUM
,TXN_POSTED_DT
,TM_POSTED_SEQ_NUM; 
"""

 # --- Execute query into DataFrame using SQLAlchemy engine ---
df = pd.read_sql(query, engine)


 # --- Build output path safely ---
current_date = datetime.today()
report_date_str = current_date.strftime('%Y%m%d')

output_dir = Path(r"C:\Users\Alison.Kao\Automation\Output")
output_dir.mkdir(parents=True, exist_ok=True)

output_path = output_dir / f"Adjustment_Transactions_{report_date_str}.xlsx"
df.to_excel(output_path, index=False, engine="openpyxl")


#--- Send Email
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
subject = "12_ADJ_TXN"
sharepoint_url = "https://bank.rogers.com/teams/DataAnalytics/03_Custom_Reports/05_Operations/Forms/AllItems.aspx?RootFolder=%2Fteams%2FDataAnalytics%2F03%5FCustom%5FReports%2F05%5FOperations%2FADJ%5FTXN&FolderCTID=0x012000356E2B2EEFE2BE4AB3ADFFD65C9A096C&View=%7BEE0C7869%2DE15F%2D44A5%2D94F3%2D6AFC5E2C0225%7D"  # <-- replace with your actual URL

# SMTP server settings (confirm with IT)
smtp_server = "smtp.rci.rogers.com"
smtp_port = 25  # if your org requires TLS/auth, switch to 587 + starttls/login

# Attach only these explicit filenames (recommended), or set to None to attach all .xlsx
expected_files = [
    f"Adjustment_Transactions_{report_date_str}.xlsx"
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

