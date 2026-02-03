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
from urllib.parse import quote_plus
import html

# --- Build SQLAlchemy connection string ---
PG_DB   = "rb_prod"
PG_USER = "rb_service_prd"
PG_PWD  = "Y[~VbY$w/E2sR+@H"  # contains special chars -> quote_plus
PG_HOST = "52.228.62.199"
PG_PORT = "5432"

PG_PWD_SAFE = quote_plus(PG_PWD)
conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PWD_SAFE}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Add connection args (connect timeout + statement timeout ~15 min)
engine = create_engine(
    conn_str,
    connect_args={
        "connect_timeout": 5,
        "options": "-c statement_timeout=900000"  # ms (900000 = 15 minutes)
    },
    future=True
)

# Quick probe: open/close a connection
try:
    with engine.connect() as con:
        print("Connected OK")
except Exception as e:
    raise RuntimeError(f"Connect failed: {e}")

# --- Split your SQL into DDL part and final SELECT ---
# 1) DDL: DROP + CREATE TABLE AS (...)
ddl_sql_raw = r"""
DROP TABLE IF EXISTS DATA_ANALYTICS.tbl_daily_app_summary;
CREATE TABLE DATA_ANALYTICS.tbl_daily_app_summary AS
(
  WITH BASE_APP AS (
      SELECT
            APP_NUM AS APPL_ID
          , DATE_TRUNC('day', DT_ENTERED) AS DT_ENTERED
      FROM DATA_ANALYTICS.TA_ADM_APP_INFO
      UNION
      SELECT
            APPL_ID
          , DATE_TRUNC('day', ENTRY_DT) AS DT_ENTERED
      FROM ADSCA_DW_ACC.APPL_CURR
      WHERE NULLIF(TRIM(APPL_STRGY_CD), '') <> 'ROGERSPQ'
  ),
  TA AS (
      SELECT
            A.APP_NUM AS APPL_ID
          , DATE_TRUNC('day', A.DT_ENTERED) AS DT_ENTERED
          , DATE_TRUNC('day', A.DT_STATUS)  AS DT_STATUS
          , CASE 
              WHEN NULLIF(TRIM(A.STATUS_FIELD), '') = 'NEWACCOUNT'                     THEN 'APPROVED'
              WHEN NULLIF(TRIM(A.STATUS_FIELD), '') IN ('DECLINE','NORESPONSE')        THEN 'DECLINED'
              WHEN NULLIF(TRIM(A.STATUS_FIELD), '') = 'REJECTOFFR'                     THEN 'REJECT OFFER'
              WHEN NULLIF(TRIM(A.STATUS_FIELD), '') = 'VOID APP'                       THEN 'VOID APP'
              WHEN NULLIF(TRIM(A.STATUS_FIELD), '') = 'WITHDRAW'                       THEN 'WITHDRAW APP'
              ELSE                                                                     'PENDING'
            END AS APP_STATUS
          , CASE 
              WHEN NULLIF(TRIM(A.ACCT_CUST_DATA_81), '') IS NULL
                  AND NULLIF(TRIM(A.DATA_ENTRY_OFFICER), '') IS NOT NULL              THEN 'OTHER'
              WHEN NULLIF(TRIM(A.ACCT_CUST_DATA_81), '') = 'TCALLCEN'
                  AND DATE_TRUNC('day', A.DT_ENTERED) 
                      BETWEEN DATE '2014-03-04' AND DATE '2014-03-19'
                  AND NULLIF(TRIM(A.USER_20_BYTE_9), '') = '24.114.224.50'            THEN 'CARE'
              WHEN NULLIF(TRIM(A.ACCT_CUST_DATA_81), '') = 'TCALLCEN'
                  AND DATE_TRUNC('day', A.DT_ENTERED) 
                      BETWEEN DATE '2014-03-04' AND DATE '2014-03-19'
                  AND NULLIF(TRIM(A.USER_20_BYTE_9), '') LIKE '24.114.255.%'          THEN 'CIS'
              WHEN NULLIF(TRIM(A.ACCT_CUST_DATA_81), '') IN (
                    'RCORRETL','RDEALER','FCORRETL','FDEALER'
                  )
                  AND NULLIF(TRIM(A.USER_75_BYTE_1), '') LIKE '%@ACTV8.%'             THEN 'INTERCEP'
              WHEN NULLIF(TRIM(A.ACCT_CUST_DATA_81), '') IN ('TCALLCEN','TMS')         THEN 'TCALLCEN'
              WHEN NULLIF(TRIM(A.ACCT_CUST_DATA_81), '') IS NULL
                  AND NULLIF(TRIM(A.DATA_ENTRY_OFFICER), '') IS NULL                  THEN 'ERROR'
              ELSE                                                                          NULLIF(TRIM(A.ACCT_CUST_DATA_81), '')
            END AS CHANNEL
          , CASE 
              WHEN NULLIF(TRIM(A.ACCT_CUST_DATA_60), '') IS NULL                       THEN 'BLANK APPLICATIONS'
              WHEN SUBSTR(NULLIF(TRIM(A.ACCT_CUST_DATA_60), ''),1,1) = '3'             THEN 'EMPLOYEE PROGRAM'
              WHEN SUBSTR(NULLIF(TRIM(A.ACCT_CUST_DATA_60), ''),1,1) IN (
                    '0','1','2','4','5','7','8','9'
                  )                                                                   THEN 'PREAPPROVED'
              ELSE                                                                          'ERROR'
            END AS CATEGORY
          , NULLIF(TRIM(A.STATUS_FIELD), '') AS STC
          , NULLIF(TRIM(A.AF_QUEUE_ID), '') AS QUEUE
          , CASE
              WHEN A.AGE_BUCKET = 'A' THEN '30+ DAYS'
              WHEN A.AGE_BUCKET = 'B' THEN '21 - 30 DAYS'
              WHEN A.AGE_BUCKET = 'C' THEN '16 - 20 DAYS'
              WHEN A.AGE_BUCKET = 'D' THEN '11 - 15 DAYS'
              WHEN A.AGE_BUCKET = 'E' THEN '6 - 10 DAYS'
              WHEN A.AGE_BUCKET = 'F' THEN 'DAY 5'
              WHEN A.AGE_BUCKET = 'G' THEN 'DAY 4'
              WHEN A.AGE_BUCKET = 'H' THEN 'DAY 3'
              WHEN A.AGE_BUCKET = 'I' THEN 'DAY 2'
              WHEN A.AGE_BUCKET = 'J' THEN 'DAY 1'
              WHEN A.AGE_BUCKET = 'K' THEN 'DAY 0'
              ELSE NULL
            END AS AGE_BUCKET
          , COALESCE(
                NULLIF(TRIM(F.CLIENT_PROD_CODE), '')
              , CASE
                  WHEN NULLIF(TRIM(DE.ALPHA_VALUE), '') = 'INCOMEWEN' THEN 'WEN'
                  WHEN NULLIF(TRIM(DE.ALPHA_VALUE), '') = 'INCOMERCB' THEN 'RCB'
                  ELSE NULL
                END
              , NULLIF(TRIM(A.CLIENT_PRODUCT_CD), '')
            ) AS ADM_CPC
          , NULLIF(TRIM(A.ACCT_CUST_DATA_62), '') AS CPGN_SRC_CD
          , CASE
              WHEN NULLIF(TRIM(A.USER_1_BYTE_12), '') = 'A'                       THEN 'ASTRUM'
              WHEN NULLIF(TRIM(A.USER_1_BYTE_12), '') = 'D'                       THEN 'DE_WEB'
              WHEN NULLIF(TRIM(A.USER_1_BYTE_12), '') IS NULL
                  OR NULLIF(TRIM(A.USER_1_BYTE_12), '') = 'I'                     THEN 'INST_APP'
              ELSE                                                                     'ERROR'
            END AS APP_ENTRY_METHOD
          , CASE
              WHEN NULLIF(TRIM(A.STATUS_FIELD), '') NOT IN (
                    'NEWACCOUNT','DECLINE','NORESPONSE','REJECTOFFR','VOID APP','WITHDRAW'
                  )                                                                   THEN 'PENDING'
              WHEN NULLIF(TRIM(A.STATUS_FIELD), '') = 'NEWACCOUNT'                    THEN 'APPROVED'
              WHEN NULLIF(TRIM(A.STATUS_FIELD), '') IN (
                    'DECLINE','NORESPONSE','REJECTOFFR','VOID APP','WITHDRAW'
                  )
                  AND NULLIF(TRIM(A.AF_QUEUE_ID), '') IN (
                        'DECSIN','DECLBUR','DECPABUR'
                      )                                                               THEN 'DECLINED_CREDIT'
              ELSE                                                                         'DECLINED_OTHER'
            END AS STC_BKDOWN
      FROM DATA_ANALYTICS.TA_ADM_APP_INFO A
      LEFT JOIN DATA_ANALYTICS.TA_ADM_DATA_ELEM DE
            ON A.APP_NUM = DE.APP_NUM
            AND NULLIF(TRIM(DE.ELEMENT_NAME), '') = 'CHKINCOME'
      LEFT JOIN DATA_ANALYTICS.TA_ADM_TS2_EXTR_OVR F
            ON A.APP_NUM = F.APP_NUM
            AND F.TS2_ACCOUNT_ID > 0
      WHERE
          NULLIF(TRIM(A.TEST_APP_SW), '')    IS NULL
      OR  NULLIF(TRIM(A.TEST_ACCT_FLAG), '') IS NULL
  ),
  ADASTRA AS (
      SELECT
            ROW_NUMBER() OVER(
                PARTITION BY A.APPL_ID
                ORDER BY A.CURR_STAT_DT DESC, A.EFFTV_FROM_DT DESC
            ) AS RANK_NUM
          , A.APPL_ID AS APPL_ID
          , DATE_TRUNC('day', A.ENTRY_DT)     AS DT_ENTERED
          , DATE_TRUNC('day', A.CURR_STAT_DT) AS DT_STATUS
          , CASE
              WHEN NULLIF(TRIM(A.APPL_STAT_CD), '') = 'NEWACCOUNT'                  THEN 'APPROVED'
              WHEN NULLIF(TRIM(A.APPL_STAT_CD), '') IN ('DECLINE','NORESPONSE')     THEN 'DECLINED'
              WHEN NULLIF(TRIM(A.APPL_STAT_CD), '') = 'REJECTOFFR'                  THEN 'REJECT OFFER'
              WHEN NULLIF(TRIM(A.APPL_STAT_CD), '') = 'VOID APP'                    THEN 'VOID APP'
              WHEN NULLIF(TRIM(A.APPL_STAT_CD), '') = 'WITHDRAW'                    THEN 'WITHDRAW APP'
              ELSE                                                                  'PENDING'
            END AS APP_STATUS
          , CASE
              WHEN NULLIF(TRIM(A.ACQSN_CHANNEL_CD), '') IS NULL
                  AND NULLIF(TRIM(B.DATA_ENTRY_OFF_NUM), '') IS NOT NULL            THEN 'OTHER'
              WHEN NULLIF(TRIM(A.ACQSN_CHANNEL_CD), '') IN ('TCALLCEN','TMS')        THEN 'TCALLCEN'
              WHEN NULLIF(TRIM(A.ACQSN_CHANNEL_CD), '') IS NULL
                  AND NULLIF(TRIM(B.DATA_ENTRY_OFF_NUM), '') IS NULL                THEN NULL
              ELSE                                                                        NULLIF(TRIM(A.ACQSN_CHANNEL_CD), '')
            END AS CHANNEL
          , CASE
              WHEN NULLIF(TRIM(G.CUSTOM_DATA_60), '') IS NULL                       THEN 'BLANK APPLICATIONS'
              WHEN SUBSTR(NULLIF(TRIM(G.CUSTOM_DATA_60), ''),1,1) = '3'             THEN 'EMPLOYEE PROGRAM'
              WHEN SUBSTR(NULLIF(TRIM(G.CUSTOM_DATA_60), ''),1,1) IN (
                    '0','1','2','4','5','7','8','9'
                  )                                                                 THEN 'PREAPPROVED'
              ELSE                                                                       'ERROR'
            END AS CATEGORY
          , NULLIF(TRIM(A.APPL_STAT_CD), '') AS STC
          , A.APPL_QUEUE_ID               AS QUEUE
          , CASE
              WHEN A.APPL_AGE_BUCKET = 'A' THEN '30+ DAYS'
              WHEN A.APPL_AGE_BUCKET = 'B' THEN '21 - 30 DAYS'
              WHEN A.APPL_AGE_BUCKET = 'C' THEN '16 - 20 DAYS'
              WHEN A.APPL_AGE_BUCKET = 'D' THEN '11 - 15 DAYS'
              WHEN A.APPL_AGE_BUCKET = 'E' THEN '6 - 10 DAYS'
              WHEN A.APPL_AGE_BUCKET = 'F' THEN 'DAY 5'
              WHEN A.APPL_AGE_BUCKET = 'G' THEN 'DAY 4'
              WHEN A.APPL_AGE_BUCKET = 'H' THEN 'DAY 3'
              WHEN A.APPL_AGE_BUCKET = 'I' THEN 'DAY 2'
              WHEN A.APPL_AGE_BUCKET = 'J' THEN 'DAY 1'
              WHEN A.APPL_AGE_BUCKET = 'K' THEN 'DAY 0'
              ELSE NULL
            END AS AGE_BUCKET
          , COALESCE(
                NULLIF(TRIM(F.CLNT_PROD_CD), '')
              , CASE
                  WHEN NULLIF(TRIM(DE.ELE_ALPH_VAL), '') = 'INCOMEWEN' THEN 'WEN'
                  WHEN NULLIF(TRIM(DE.ELE_ALPH_VAL), '') = 'INCOMERCB' THEN 'RCB'
                  ELSE NULL
                END
              , NULLIF(TRIM(G.CLNT_PROD_CD), '')
            ) AS ADM_CPC
          , NULLIF(TRIM(G.CUSTOM_DATA_62), '') AS CPGN_SRC_CD
          , CASE
              WHEN E.USER_1_BYTE_12 = 'A'                          THEN 'ASTRUM'
              WHEN E.USER_1_BYTE_12 = 'D'                          THEN 'DE_WEB'
              WHEN E.USER_1_BYTE_12 IS NULL OR E.USER_1_BYTE_12='I' THEN 'INST_APP'
              ELSE                                                    'ERROR'
            END AS APP_ENTRY_METHOD
          , CDF.CUSTOM_21_DATA            AS INST_CR
          , CASE
              WHEN NULLIF(TRIM(A.APPL_STAT_CD), '') NOT IN (
                    'NEWACCOUNT','DECLINE','NORESPONSE','REJECTOFFR','VOID APP','WITHDRAW'
                  )                                                                  THEN 'PENDING'
              WHEN NULLIF(TRIM(A.APPL_STAT_CD), '') = 'NEWACCOUNT'                   THEN 'APPROVED'
              WHEN NULLIF(TRIM(A.APPL_STAT_CD), '') IN (
                    'DECLINE','NORESPONSE','REJECTOFFR','VOID APP','WITHDRAW'
                  )
                  AND NULLIF(TRIM(A.APPL_QUEUE_ID), '') IN (
                        'DECSIN','DECLBUR','DECPABUR'
                      )                                                              THEN 'DECLINED_CREDIT'
              ELSE                                                                        'DECLINED_OTHER'
            END AS STC_BKDOWN
      FROM ADSCA_DW_ACC.APPL_CURR A
      LEFT JOIN ADSCA_DW_ACC.APPL_DCSN_CURR B
            ON A.APPL_ID = B.APPL_ID
            AND B.EFFTV_FROM_DT <= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
            AND B.EFFTV_TO_DT   >= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
      LEFT JOIN ADSCA_DW_ACC.APPL_DATA_EL_CURR DE
            ON A.APPL_ID = DE.APPL_ID
            AND NULLIF(TRIM(DE.ELE_ELMT_NM), '') = 'CHKINCOME'
            AND DE.EFFTV_FROM_DT <= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
            AND DE.EFFTV_TO_DT   >= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
      LEFT JOIN ADSCA_DW_ACC.APPL_USER_CUST_DATA_CURR E
            ON A.APPL_ID = E.APPL_ID
            AND E.EFFTV_FROM_DT <= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
            AND E.EFFTV_TO_DT   >= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
      LEFT JOIN ADSCA_DW_ACC.APPL_XTO_GENL_ACCT_CURR F
            ON A.APPL_ID = F.APPL_ID
            AND F.ACCT_ID > 0
            AND F.EFFTV_FROM_DT <= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
            AND F.EFFTV_TO_DT   >= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
      LEFT JOIN ADSCA_DW_ACC.APPL_GENL_ACCT_CURR G
            ON A.APPL_ID = G.APPL_ID
            AND G.EFFTV_FROM_DT <= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
            AND G.EFFTV_TO_DT   >= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
      LEFT JOIN ADSCA_DW_ACC.ACCT_CUSTOM_DATA_CURR CDF
            ON CDF.EFFTV_FROM_DT <= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
            AND CDF.EFFTV_TO_DT   >= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
            AND CDF.ACCT_ID       = F.ACCT_ID
            AND CDF.ACCT_SEQ_NUM  = 0
            AND CDF.CUSTOM_21_DATA IN ('EI','IC','IP','VC','VF','VP')
      WHERE
          A.EFFTV_FROM_DT <= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
      AND A.EFFTV_TO_DT   >= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'
  ),
  OVERALL AS (
      SELECT DISTINCT
            BASE_APP.APPL_ID
          , COALESCE(ADASTRA.DT_ENTERED, TA.DT_ENTERED) AS DT_ENTERED
          , CASE
              WHEN COALESCE(ADASTRA.DT_STATUS, '1900-01-01'::timestamp)
                  >= COALESCE(TA.DT_STATUS, '1900-01-01'::timestamp)
              THEN COALESCE(ADASTRA.DT_STATUS, TA.DT_STATUS)
              ELSE COALESCE(TA.DT_STATUS, ADASTRA.DT_STATUS)
            END AS DT_STATUS
          , CASE
              WHEN COALESCE(ADASTRA.DT_STATUS, '1900-01-01'::timestamp)
                  >= COALESCE(TA.DT_STATUS, '1900-01-01'::timestamp)
              THEN COALESCE(ADASTRA.APP_STATUS, TA.APP_STATUS)
              ELSE COALESCE(TA.APP_STATUS, ADASTRA.APP_STATUS)
            END AS APP_STATUS
          , CASE
              WHEN COALESCE(ADASTRA.CHANNEL, TA.CHANNEL) = 'INTERNET'
                  AND COALESCE(ADASTRA.CPGN_SRC_CD, TA.CPGN_SRC_CD) = 'IDVPMO'
                  AND COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) IN ('WEN','RCB','CCA')
              THEN 'RDEALER'
              WHEN COALESCE(ADASTRA.CHANNEL, TA.CHANNEL) = 'INTERNET'
                  AND COALESCE(ADASTRA.CPGN_SRC_CD, TA.CPGN_SRC_CD) = 'IDVPMO'
                  AND COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'FCB'
              THEN 'FDEALER'
              ELSE COALESCE(ADASTRA.CHANNEL, TA.CHANNEL)
            END AS CHANNEL
          , COALESCE(ADASTRA.CATEGORY, TA.CATEGORY) AS CATEGORY
          , CASE
              WHEN COALESCE(ADASTRA.DT_STATUS, '1900-01-01'::timestamp)
                  >= COALESCE(TA.DT_STATUS, '1900-01-01'::timestamp)
              THEN COALESCE(ADASTRA.STC, TA.STC)
              ELSE COALESCE(TA.STC, ADASTRA.STC)
            END AS STC
          , COALESCE(ADASTRA.QUEUE, TA.QUEUE)          AS QUEUE
          , COALESCE(ADASTRA.AGE_BUCKET, TA.AGE_BUCKET) AS AGE_BUCKET
          , COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC)       AS ADM_CPC
          , CASE
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'CON'                    THEN 'CONNECTIONS'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'FCB'                    THEN 'FIDO'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'WEN'                    THEN 'WORLD ELITE'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'TEC'                    THEN 'CORPORATE EXPENSE'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'PIC'                    THEN 'CORPORATE PURCHASE'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'WRD'                    THEN 'WORLD'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'WLC'                    THEN 'WORLD LEGEND'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'SMB'                    THEN 'SMALL BUSINESS'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) IN ('RCB','CCA')           THEN 'PLATINUM'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'CCB'                    THEN 'CHATR STANDARD'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'CSC'                    THEN 'CHATR SECURED'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) = 'RRR'                    THEN 'FIRST REWARDS'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) IS NULL
                  AND COALESCE(ADASTRA.DT_ENTERED, TA.DT_ENTERED) < DATE '2014-08-01'
              THEN 'FIRST REWARDS'
              WHEN COALESCE(ADASTRA.ADM_CPC, TA.ADM_CPC) IS NULL
                  AND COALESCE(ADASTRA.DT_ENTERED, TA.DT_ENTERED) >= DATE '2014-08-01'
              THEN 'PLATINUM'
              ELSE 'ERROR'
            END AS PRODUCT_TYPE
          , COALESCE(ADASTRA.APP_ENTRY_METHOD, TA.APP_ENTRY_METHOD) AS APP_ENTRY_METHOD
          , CASE 
              WHEN ADASTRA.INST_CR IS NOT NULL THEN 'IC'
              ELSE 'NON_IC'
            END AS INST_CR
          , CASE
              WHEN COALESCE(ADASTRA.DT_STATUS, '1900-01-01'::timestamp)
                  >= COALESCE(TA.DT_STATUS, '1900-01-01'::timestamp)
              THEN COALESCE(ADASTRA.STC_BKDOWN, TA.STC_BKDOWN)
              ELSE COALESCE(TA.STC_BKDOWN, ADASTRA.STC_BKDOWN)
            END AS STC_BKDOWN
      FROM BASE_APP
      LEFT JOIN TA
            ON BASE_APP.APPL_ID = TA.APPL_ID
      LEFT JOIN ADASTRA
            ON BASE_APP.APPL_ID = ADASTRA.APPL_ID
            AND ADASTRA.RANK_NUM = 1
      WHERE
            DATE_TRUNC('day', BASE_APP.DT_ENTERED) < DATE_TRUNC('day', NOW())
        AND BASE_APP.APPL_ID NOT IN (
              SELECT APP_NUM
              FROM DATA_ANALYTICS.D_PRD_VERIF_APP_NUM
            )
"""

 # --- Execute query into DataFrame using SQLAlchemy engine ---
df = pd.read_sql(query, engine)


 # --- Build output path safely ---
current_date = datetime.today()
report_date_str = current_date.strftime('%Y%m%d')

output_dir = Path(r"C:\Users\Alison.Kao\Automation\Output")
output_dir.mkdir(parents=True, exist_ok=True)

output_path = output_dir / f"tbl_daily_app_summary_{report_date_str}.xlsx"
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
    "alison.kao@rci.rogers.com"
]

# Subject + SharePoint link in the body
subject = "14_DAILY_APP_SUMMARY_UPDATE"
sharepoint_url = "https://bank.rogers.com/teams/DataAnalytics/06_Code_Repository/02_Peter/Forms/AllItems.aspx?RootFolder=%2Fteams%2FDataAnalytics%2F06%5FCode%5FRepository%2F02%5FPeter%2FPostgreSQL%20Code%2F01%20Daily%20Jobs%2F14%5FDAILY%5FAPPS&FolderCTID=0x0120008C654226E7BF3546B1ABCFBEB2357AA1&View=%7BA1248A26%2D0436%2D4B25%2DADB8%2DBCEA98946F65%7D"  # <-- replace with your actual URL

# SMTP server settings (confirm with IT)
smtp_server = "smtp.rci.rogers.com"
smtp_port = 25  # if your org requires TLS/auth, switch to 587 + starttls/login

# Attach only these explicit filenames (recommended), or set to None to attach all .xlsx
expected_files = [
    f"tbl_daily_app_summary_{report_date_str}.xlsx"
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

