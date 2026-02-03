# -*- coding: utf-8 -*-
"""
Created on Mon Dec 15 13:17:34 2025

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


# --- Your query ---

query = """
    WITH ref_date AS (
          SELECT 
            TO_CHAR(CURRENT_DATE - INTERVAL '1 DAY', 'YYYY_MM') AS target_month,
            CURRENT_DATE - INTERVAL '1 DAY' AS target_date
        ),
        
        delta_raw AS (
          SELECT
            a.acct_id,
            a.acct_seq_num,
            a.acct_open_dt,
            a.cpc,
            a.inst_credit AS cdf21,
            a.lastday_month::DATE AS data_bus_dt,
            CURRENT_DATE AS data_load_dt
          FROM data_analytics.pl_last_acct_stat a
          CROSS JOIN ref_date
          WHERE a.eff_dt_yymm = ref_date.target_month
            AND a.inst_credit IN ('IP', 'IC', 'VP', 'VC', 'VF')
            AND a.acct_seq_num = 0
            AND NOT EXISTS (
              SELECT 1
              FROM data_analytics.cp_strike_inst_cr b
              WHERE b.acct_id = a.acct_id
            )
        ),
        
        inserted_deltas AS (
          INSERT INTO data_analytics.cp_strike_inst_cr (
            acct_id,
            acct_seq_num,
            acct_open_dt,
            cpc,
            cdf21,
            data_bus_dt,
            data_load_dt
          )
          SELECT
            acct_id,
            acct_seq_num,
            acct_open_dt,
            cpc,
            cdf21,
            data_bus_dt,
            data_load_dt
          FROM delta_raw
          RETURNING *
        ),
        
        final_output AS (
          SELECT
            e.name,
            e.name_emboss,
            e.state_prov_code as prov,
            e.zip_postal_code as postal_code,
            d.cpc,
            d.cdf21,
            d.acct_open_dt::DATE,
            LPAD(d.acct_id::TEXT, 11, '0') AS acct_id
          FROM inserted_deltas D
          CROSS JOIN ref_date
          LEFT JOIN adsca_dw_acc.acct_mstr_am0e_curr e
            ON d.acct_id = e.acct_id
           AND d.acct_seq_num = e.acct_seq_num
           AND e.efftv_from_dt <= ref_date.target_date
           AND e.efftv_to_dt >= ref_date.target_date
        )
        
        SELECT * 
        FROM final_output;
        """
 # --- Execute query into DataFrame using SQLAlchemy engine ---
df = pd.read_sql(query, engine)

 # --- Build output path safely ---
current_date = datetime.today()
report_date_str = current_date.strftime('%Y%m%d')

output_dir = Path(r"C:\Users\Alison.Kao\Automation\Output")
output_dir.mkdir(parents=True, exist_ok=True)

output_path = output_dir / f"CP_INST_CR_{report_date_str}.xlsx"
df.to_excel(output_path, index=False, engine="openpyxl")

print(f"Rows exported: {len(df)}")
print(f"Saved: {output_path}")

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
subject = "07_INST_CR"
sharepoint_url = "https://bank.rogers.com/teams/DataAnalytics/03_Custom_Reports/05_Operations/Forms/AllItems.aspx?RootFolder=%2Fteams%2FDataAnalytics%2F03%5FCustom%5FReports%2F05%5FOperations%2FCANADA%5FPOST%5FINST%5FCR&FolderCTID=0x012000356E2B2EEFE2BE4AB3ADFFD65C9A096C&View=%7BEE0C7869%2DE15F%2D44A5%2D94F3%2D6AFC5E2C0225%7D"  # <-- replace with your actual URL

# SMTP server settings (confirm with IT)
smtp_server = "smtp.rci.rogers.com"
smtp_port = 25  # if your org requires TLS/auth, switch to 587 + starttls/login

# Attach only these explicit filenames (recommended), or set to None to attach all .xlsx
expected_files = [
    f"CP_INST_CR_{report_date_str}.xlsx"
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
