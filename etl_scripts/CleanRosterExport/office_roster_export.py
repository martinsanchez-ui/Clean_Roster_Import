#################################################################################################################
#	Author: Chris O'Brien   
#	Created On: Wed Apr 15 2020
#	File: office_roster_export.py
#	Description: Clean Office Roster Export
#   Google Sheet: https://docs.google.com/spreadsheets/d/1YfYfUotaHAD6G76RFWM9hVFZCBHzMnpy5UevR3sgCI0/edit#gid=0
#################################################################################################################
import datetime
import os
import sys
import time
import traceback

import gspread
import MySQLdb as mysql
import pandas as pd
import pandas.io.formats.excel
import requests
from dotenv import load_dotenv
from exchangelib import Account, Configuration, FileAttachment, Identity, IMPERSONATION, Mailbox, Message, OAuth2Credentials
from exchangelib.version import EXCHANGE_O365, Version
from oauth2client.service_account import ServiceAccountCredentials
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from unidecode import unidecode

from etl_utilities.RMLogger import Logger
from etl_utilities.utils import send_teams_message, load_secrets


load_dotenv()

SCRIPT_PATH = sys.path[0] + os.sep
ENV_FILE_PATH = os.path.join(os.path.sep, "datafiles", "app_credentials.env")
GOOGLE_SHEET_DATA_FILE = os.path.join(SCRIPT_PATH, "sheet_data.dat")
BACKUP_DIR = "backups"
LOG_LOCATION = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "log.log")
RETRY_ATTEMPTS = 3
RETRY_PAUSE = 5
SEND_TEAMS_MESSAGE_SUMMARY = "Clean_Roster_Export"
SEND_TEAMS_MESSAGE_ACTIVITY_TITLE = "Error occurred - Clean_Roster_Export"
FULL_DRY_RUN = True  # TEMP - flip to False for production

SECRETS = load_secrets(ENV_FILE_PATH, ["bi_hostname", "bi_username", "bi_password", "bi_database", 
                                       "client_id", "client_secret", "tenant_id", "primary_smtp_address", "outlook_server", 
                                       "set_office_status_url", "set_office_status_api_key"])


## Config data
pd.options.display.width = 0

log = Logger(log_file_location=LOG_LOCATION, log_file_backup_count=50, logging_level="DEBUG")
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

is_python3 = sys.version_info.major == 3
if is_python3:
    unicode = str

log.info("Beginning processing...")
start_ts = time.time()

credentials = OAuth2Credentials(client_id=SECRETS.get("client_id"), client_secret=SECRETS.get("client_secret"), tenant_id=SECRETS.get("tenant_id"),
                                identity=Identity(primary_smtp_address=SECRETS.get("primary_smtp_address")))
config = Configuration(credentials=credentials, server=SECRETS.get("outlook_server"), version=Version(build=EXCHANGE_O365))
account = Account(primary_smtp_address=SECRETS.get("primary_smtp_address"), config=config, autodiscover=False, access_type=IMPERSONATION)

log.info("Opening database connection...")
db = mysql.connect(SECRETS.get("bi_hostname"), SECRETS.get("bi_username"), SECRETS.get("bi_password"), SECRETS.get("bi_database"), charset='utf8')
log.info("Database connection open.")


def send_teams_message_safe(*args, **kwargs):
    if FULL_DRY_RUN:
        log.info("DRY_RUN: would send Teams message")
        return
    return send_teams_message(*args, **kwargs)


def send_email(account, subject, body, recipients, attachments=None):
    """
    Send an email.

    Parameters
    ----------
    account : Account object
    subject : str
    body : str
    recipients : list of str
        Each str is an email address
    attachments : list of tuples or None
        (filename, binary contents)

    Examples
    --------
    >>> send_email(account, 'Subject line', 'Hello!', ['info@example.com'])
    """
    if FULL_DRY_RUN:
        log.info("DRY_RUN: would send email")
        return
    to_recipients = [Mailbox(email_address=recipients)]
    print(to_recipients)

    # Create message
    m = Message(account=account,
                folder=account.sent,
                subject=subject,
                body=body,
                to_recipients=to_recipients)

    # Attach files
    for attachment_name, attachment_content in attachments or []:
        file = FileAttachment(name=attachment_name, content=attachment_content)
        m.attach(file)
    m.send_and_save()

def grab_office_list_just_webinar():
    log.info("Entering {}()".format(sys._getframe().f_code.co_name))

    webinar_sql = """
        SELECT
            a.company_id AS office_id,
            a.on_what_id AS workflow_id,
            MIN(a.entered_at) AS activity_created_at
        
        FROM
            xrms.activities a
            JOIN bi_warehouse_prd.activity_template_d ac ON (a.activity_template_id=ac.template_id)
            JOIN bi_warehouse_prd.office_d od ON (a.company_id=od.office_id)
            LEFT JOIN xrms.activities a2 ON (a.company_id=a2.company_id AND a2.activity_template_id=910 AND a2.activity_record_status='a' AND (a2.completed_at=0 OR a2.completed_at IS NULL))
            LEFT JOIN xrms.opportunities o ON (a2.on_what_id=o.opportunity_id AND a2.on_what_table='opportunities' AND o.opportunity_record_status='a')
        
        WHERE
            a.company_id NOT IN (
                SELECT DISTINCT(ora.office_id)
                FROM bi_warehouse_prd.office_roster_automation_f ora
                JOIN xrms.companies c ON (ora.office_id=c.company_id)
                WHERE 
                    c.roster_reviewed_on >= CURRENT_TIMESTAMP() - INTERVAL 6 MONTH 
                    OR ora.sent_dtt >= CURRENT_TIMESTAMP() - INTERVAL 6 MONTH
            )
            AND a.activity_template_id IN (2476, 606, 3110, 2871)
            AND a.activity_record_status = 'a'
            AND a.activity_status = 'o'
            AND od.open_webinar_status_id = 334
            AND od.primary_network_id <> 52
            AND od.franchise_network_id <> 52
            AND od.highest_primary_network_id <> 52
            AND od.highest_network_id <> 52

        GROUP BY
            a.company_id

        ;
    """

    office_webinar_list_df = pd.read_sql(webinar_sql, db)
    return office_webinar_list_df

def grab_office_list_with_meets_at_relationship():
    log.info("Entering {}()".format(sys._getframe().f_code.co_name))

    meets_at_relationship_query = """
    SELECT
        derived_table.office_id,
        derived_table.workflow_id,
        derived_table.host_office_id,
        derived_table.relationship_type,
        derived_table.activity_created_at
    
    FROM (
            SELECT
                    a.company_id AS office_id,
                    a.on_what_id AS workflow_id,
                    o.company_id AS host_office_id,
                    'meets_at_opportunity' AS relationship_type,
                    MIN(a.entered_at) AS activity_created_at
                
                FROM
                    xrms.activities a
                    JOIN bi_warehouse_prd.office_d od ON (a.company_id = od.office_id)
                    JOIN xrms.company_meets_at_opportunities c FORCE INDEX(company_idx) ON (a.company_id = c.company_id AND c.row_status = 'a')
                    JOIN xrms.opportunities o ON (c.opportunity_id = o.opportunity_id AND o.opportunity_record_status = 'a')
                    JOIN xrms.activities a2 ON (c.opportunity_id = a2.on_what_id AND a2.on_what_table = 'opportunities' AND a2.activity_template_id = 910 AND a2.activity_record_status = 'a' AND (a2.completed_at = 0 OR a2.completed_at IS NULL))
                
                WHERE
                    a.activity_template_id IN (2476, 606, 3110, 2871)
                    AND a.company_id NOT IN (
                        SELECT DISTINCT(ora.office_id)
                        FROM bi_warehouse_prd.office_roster_automation_f ora
                        JOIN xrms.companies c ON (ora.office_id=c.company_id)
                        WHERE 
                            c.roster_reviewed_on >= CURRENT_TIMESTAMP() - INTERVAL 6 MONTH 
                            OR ora.sent_dtt >= CURRENT_TIMESTAMP() - INTERVAL 6 MONTH
                    )
                    AND a.activity_record_status = 'a'
                    AND a.activity_status = 'o'
                    AND od.primary_network_id <> 52
                    AND od.franchise_network_id <> 52
                    AND od.highest_primary_network_id <> 52
                    AND od.highest_network_id <> 52
                
                GROUP BY
                    a.company_id

                UNION 

                SELECT
                    od.office_id AS office_id,
                    a.on_what_id AS workflow_id,
                    od.meets_at_office_id AS host_office_id,
                    'meets_at_office' AS relationship_type,
                    MIN(a.entered_at) AS activity_created_at
                
                FROM
                    xrms.activities a
                    JOIN bi_warehouse_prd.activity_template_d ac ON (a.activity_template_id = ac.template_id)
                    JOIN bi_warehouse_prd.office_d od ON (a.company_id = od.office_id)
                    JOIN xrms.activities a2 ON (od.meets_at_office_id = a2.company_id AND od.office_id<>a2.company_id AND a2.activity_template_id = 910 AND a2.activity_record_status = 'a' AND (a2.completed_at = 0 OR a2.completed_at IS NULL))
                
                WHERE
                    a.company_id NOT IN (
                        SELECT DISTINCT(ora.office_id)
                        FROM bi_warehouse_prd.office_roster_automation_f ora
                        JOIN xrms.companies c ON (ora.office_id=c.company_id)
                        WHERE 
                            (c.roster_reviewed_on >= CURRENT_TIMESTAMP() - INTERVAL 6 MONTH OR 
                            ora.sent_dtt >= CURRENT_TIMESTAMP() - INTERVAL 6 MONTH)
                    )
                    AND a.activity_template_id IN (2476, 606, 3110, 2871)
                    AND a.activity_record_status = 'a'
                    AND a.activity_status = 'o'
                    AND od.office_record_status = 'Active'
                    AND od.primary_network_id <> 52
                    AND od.franchise_network_id <> 52
                    AND od.highest_primary_network_id <> 52
                    AND od.highest_network_id <> 52
                
                GROUP BY
                    a.company_id
            
            ) AS derived_table
    ;
    """

    office_meets_at_list_df = pd.read_sql(meets_at_relationship_query, db)
    return office_meets_at_list_df

def get_advocate_relationship():
    log.info("Entering {}()".format(sys._getframe().f_code.co_name))

    office_advocate_sql = f"""
        SELECT
            a.company_id AS office_id,
            a.on_what_id AS workflow_id,
            CASE
                WHEN a.company_id = r.from_id THEN r.to_id
                ELSE r.from_id
            END AS host_office_id,
            r.type_id AS relationship_type,
            MIN(a.entered_at) AS activity_created_at
        
        FROM
            xrms.activities a
            JOIN bi_warehouse_prd.activity_template_d ac ON (a.activity_template_id=ac.template_id)
            JOIN bi_warehouse_prd.office_d od ON (a.company_id=od.office_id)
            JOIN xrms.relationships r ON ((a.company_id=r.from_id OR a.company_id=r.to_id) AND r.ended_on=0)
            LEFT JOIN xrms.activities a2 ON (a.company_id=a2.company_id AND a2.activity_template_id=910 AND a2.activity_record_status='a' AND (a2.completed_at=0 OR a2.completed_at IS NULL))
            LEFT JOIN xrms.opportunities o ON (a2.on_what_id=o.opportunity_id AND a2.on_what_table='opportunities' AND o.opportunity_record_status='a')
        
        WHERE
            a.company_id NOT IN (
                SELECT DISTINCT(ora.office_id)
                FROM bi_warehouse_prd.office_roster_automation_f ora
                JOIN xrms.companies c ON (ora.office_id=c.company_id)
                WHERE 
                    c.roster_reviewed_on >= CURRENT_TIMESTAMP() - INTERVAL 6 MONTH 
                    OR ora.sent_dtt >= CURRENT_TIMESTAMP() - INTERVAL 6 MONTH
            )
            AND a.activity_template_id IN (2476, 606, 3110, 2871)
            AND a.activity_record_status = 'a'
            AND a.activity_status = 'o'
            AND od.open_webinar_status_id <> 334
            AND r.type_id IN (91, 92)
            AND od.primary_network_id <> 52
            AND od.franchise_network_id <> 52
            AND od.highest_primary_network_id <> 52
            AND od.highest_network_id <> 52
        
        GROUP BY
            a.company_id

        ;
    """

    office_advocate_list = pd.read_sql(office_advocate_sql, db)
    return office_advocate_list

def get_office_list():
    log.info("Entering {}()".format(sys._getframe().f_code.co_name))

    office_list_sql = f"""
    SELECT
        a.company_id AS office_id,
        a.on_what_id AS workflow_id,
        MIN(a.entered_at) AS activity_created_at
    FROM
        xrms.activities a
        JOIN bi_warehouse_prd.activity_template_d ac ON (a.activity_template_id=ac.template_id)
        JOIN bi_warehouse_prd.office_d od ON (a.company_id=od.office_id)
        LEFT JOIN xrms.activities a2 ON (a.company_id=a2.company_id AND a2.activity_template_id=910 AND a2.activity_record_status='a' AND (a2.completed_at=0 OR a2.completed_at IS NULL))
        LEFT JOIN xrms.opportunities o ON (a2.on_what_id=o.opportunity_id AND a2.on_what_table='opportunities' AND o.opportunity_record_status='a')
    WHERE
        a.company_id NOT IN (
            SELECT DISTINCT(ora.office_id)
            FROM bi_warehouse_prd.office_roster_automation_f ora
            JOIN xrms.companies c ON (ora.office_id=c.company_id)
            WHERE 
                c.roster_reviewed_on >= CURRENT_TIMESTAMP() - INTERVAL 6 MONTH 
                OR ora.sent_dtt >= CURRENT_TIMESTAMP() - INTERVAL 6 MONTH
        )
        AND a.activity_template_id IN (2476, 606, 3110, 2871)
        AND a.activity_record_status = 'a'
        AND a.activity_status = 'o'
        AND od.open_webinar_status_id <> 334
        AND od.primary_network_id <> 52
        AND od.franchise_network_id <> 52
        AND od.highest_primary_network_id <> 52
        AND od.highest_network_id <> 52
    GROUP BY
        a.company_id
    """

    office_list_df = pd.read_sql(office_list_sql, db)
    return office_list_df

def get_current_webinar_info(office_ids):
    log.info("Entering {}()".format(sys._getframe().f_code.co_name))

    if not office_ids:
        return pd.DataFrame(columns=[
            "office_id",
            "webinar_date",
            "opportunity_last_updated"
        ])

    office_id_list = ", ".join(str(int(office_id)) for office_id in set(office_ids))
    webinar_info_sql = f"""
        SELECT
            a.company_id AS office_id,
            MIN(COALESCE(b.prep_datetime_start, a.scheduled_at)) AS webinar_date,
            MAX(o.last_modified_at) AS opportunity_last_updated
        FROM
            xrms.activities a
            JOIN xrms.opportunities o ON (a.on_what_id = o.opportunity_id AND a.on_what_table = 'opportunities' AND o.opportunity_record_status = 'a')
            LEFT JOIN xrms.booking_presentations b ON (o.opportunity_id = b.opportunity_id AND b.row_status = 'a')
        WHERE
            a.activity_template_id = 910
            AND a.activity_record_status = 'a'
            AND a.activity_status = 'o'
            AND (a.completed_at = 0 OR a.completed_at IS NULL)
            AND a.company_id IN ({office_id_list})
        GROUP BY
            a.company_id
        ;
    """

    webinar_info_df = pd.read_sql(webinar_info_sql, db)
    return webinar_info_df

def set_roster_status(workflow_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    if FULL_DRY_RUN:
        log.info("DRY_RUN: would set status")
        return

    log.info("Setting roster status to 1441 (Third Party Cleaning)...")

    params = {
        "api_key": SECRETS.get("set_office_status_api_key"),
        "workflows": [{
                    "workflow_id": workflow_id,
                    "workflow_type": "case",
                    "status_id": 1441
                }
            ]
    }

    r = requests.post(SECRETS.get("set_office_status_url"), json=params, verify=False)
    log.info(r.text)
    if r.status_code != 200:
        message = "Status code is: {sc}, for update roster status. Please investigate".format(sc=r.status_code)
        send_teams_message_safe(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                                activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=message)


def update_stats_table(params):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    if FULL_DRY_RUN:
        log.info("DRY_RUN: would insert stats")
        return

    insert_sql = """INSERT IGNORE INTO bi_warehouse_prd.office_roster_automation_f
(office_id, workflow_id, sent_records, sent_to_email, sent_filename, sent_dtt, received_records, received_records_not_found, received_records_update, received_records_insert, received_from_email, received_filename, received_dtt, added_ts, last_updated_ts)
VALUES(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());"""

    insert_cur = db.cursor()

    log.info("Inserting records into stats table...")

    try:
        insert_cur.execute(insert_sql, params)
        db.commit()
        log.info("Insert successful.")
    except mysql.Error as e:
        log.error("Insert failed with MySQL error: {e}".format(e=e))
        log.error("Attempted query: {q}".format(q=insert_cur._last_executed))

    insert_cur.close()

def clean_phone_number(phone_number):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    if ".0" in str(phone_number)[-2:]:
        ## Fix the Pandas 'long' issue
        phone_number = str(int(phone_number))

    clean_phone = "".join(c for c in str(phone_number) if c.isdigit())
    if len(clean_phone.strip()) > 0:
        return(str(clean_phone))
    else:
        return ""

def clean_unicode(text_str):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    
    # log.info("Processing: {text_str}".format(text_str=text_str))

    # return text_str.decode("utf-8", "ignore").strip()
    
    if text_str is None:
        ##  handle null fields
        return "N/A"
    else:
        return unidecode(text_str).strip()

def export_office_roster(office_id, workflow_id, email_address):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    log.info("Exporting office roster for Office ID: {oid}".format(oid=office_id))

    filename = "{oid}_{dt}.xlsx".format(oid=office_id, dt=datetime.datetime.today().strftime("%Y_%m_%d"))

    log.info("Export filename: {sd}".format(sd=os.path.join(SCRIPT_PATH, filename)))

    office_roster_sql = """
        SELECT
            '' AS 'Not Found',
            cd.contact_id,
            od.office_id,
            IF(cd.nickname<>'',cd.nickname,cd.first_name) AS first_name,
            cd.last_name,
            cd.gender_short AS gender,
            IF(cd.title='No Title','',cd.title) AS title,
            IFNULL(cci.contactinfo,'') AS cell_phone,
            IFNULL(cci3.contactinfo,'') AS direct_phone,
            IFNULL(cci2.contactinfo,'') AS email,
            '' AS photo_url,
            '' AS linkedin_url,
            '' AS facebook_url,
            '' AS twitter_url,
            '' AS zillow_url,
            '' AS trulia_url,
            od.primary_network,
            od.franchise_network,
            cw.website_url AS roster_url,
            n.website AS website_1,
            n2.website AS website_2,
            od.line_1,
            od.city,
            od.state_abbr,
            od.zip_code

        FROM
            bi_warehouse_prd.office_d od
            JOIN bi_warehouse_prd.contact_d cd ON(od.office_id=cd.office_id)
            LEFT JOIN xrms.relationships xr ON(cd.contact_id=xr.from_id)
            LEFT JOIN xrms.contacts_contactinfo cci ON(cd.contact_id=cci.userid AND cci.contacttype='Cell' AND cci.row_status='a')
            LEFT JOIN xrms.contacts_contactinfo cci2 ON(cd.contact_id=cci2.userid AND cci2.contacttype='Email' AND cci2.row_status='a')
            LEFT JOIN xrms.contacts_contactinfo cci3 ON(cd.contact_id=cci3.userid AND cci3.contacttype='Direct' AND cci3.row_status='a')
            LEFT JOIN xrms.company_websites cw ON(od.office_id=cw.company_id AND cw.row_status='a' AND cw.label_id=2)
            LEFT JOIN xrms.networks n ON(od.primary_network_id=n.network_id)
            LEFT JOIN xrms.networks n2 ON(od.franchise_network_id=n2.network_id)

        WHERE
            od.office_id = {oid}
            AND cd.contact_record_status='Active'
            AND (xr.type_id IS NULL OR xr.type_id <> 10)
            AND cd.contact_id NOT IN (SELECT xr2.from_id FROM xrms.relationships xr2 WHERE xr2.type_id=10 AND xr2.ended_on=0)
            AND cd.title <> 'Assistant'
            AND od.primary_network_id <> 52
            AND od.franchise_network_id <> 52
            AND od.highest_primary_network_id <> 52
            AND od.highest_network_id <> 52
        
        GROUP BY
            cd.contact_id
        
        ORDER BY
            IF (cd.last_name = '' OR cd.last_name IS NULL,1,0), cd.last_name, cd.first_name
        ;
    """

    log.info("Getting office roster data...")

    office_roster_full_df = pd.read_sql(office_roster_sql.format(oid=office_id), db)

    log.info("Done.  Records found: {rf}".format(rf=len(office_roster_full_df.index)))

    office_roster_full_df["first_name"] = office_roster_full_df["first_name"].apply(clean_unicode)
    office_roster_full_df["last_name"] = office_roster_full_df["last_name"].apply(clean_unicode)
    office_roster_full_df["title"] = office_roster_full_df["title"].apply(clean_unicode)
    office_roster_full_df["primary_network"] = office_roster_full_df["primary_network"].apply(clean_unicode)
    office_roster_full_df["franchise_network"] = office_roster_full_df["franchise_network"].apply(clean_unicode)
    office_roster_full_df["email"] = office_roster_full_df["email"].apply(clean_unicode)
    
    
    if len(office_roster_full_df.index) == 0:
        log.info("No contact records found for office.  Exporting blank row and headers...")
        office_roster_sql = """
        SELECT
            '' AS 'Not Found',
            '' AS contact_id,
            od.office_id,
            '' AS first_name,
            '' AS last_name,
            '' AS gender,
            '' AS title,
            '' AS cell_phone,
            '' AS direct_phone,
            '' AS email,
            '' AS photo_url,
            '' AS linkedin_url,
            '' AS facebook_url,
            '' AS twitter_url,
            '' AS zillow_url,
            '' AS trulia_url,
            od.primary_network,
            od.franchise_network,
            cw.website_url AS roster_url,
            n.website AS website_1,
            n2.website AS website_2,
            od.line_1,
            od.city,
            od.state_abbr,
            od.zip_code
        FROM
            bi_warehouse_prd.office_d od
            LEFT JOIN xrms.company_websites cw ON(od.office_id=cw.company_id AND cw.row_status='a' AND cw.label_id=2)
            LEFT JOIN xrms.networks n ON(od.primary_network_id=n.network_id)
            LEFT JOIN xrms.networks n2 ON(od.franchise_network_id=n2.network_id)

        WHERE
            od.office_id = {oid}
            AND od.primary_network_id <> 52
            AND od.franchise_network_id <> 52
            AND od.highest_primary_network_id <> 52
            AND od.highest_network_id <> 52
        ;
    """

        log.info("Getting office roster data...")
        office_roster_full_df = pd.read_sql(office_roster_sql.format(oid=office_id), db)
        log.info("Done.")

    log.info("Configuring data and header rows...")
    data_rows_df = office_roster_full_df.iloc[:, :15]
    data_rows_df["cell_phone"] = data_rows_df["cell_phone"].apply(clean_phone_number) #.astype("int64")
    # data_rows_df["cell_phone"] = data_rows_df["cell_phone"].replace(0, "")
    data_rows_df["direct_phone"] = data_rows_df["direct_phone"].apply(clean_phone_number) #.astype("int64")
    # data_rows_df["direct_phone"] = data_rows_df["direct_phone"].replace(0, "")
    header_rows_df = office_roster_full_df.iloc[:1, 15:]

    primary_network_label = "Primary Network"
    franchise_network_label = "Franchise Network"
    roster_url_label = "Roster Url"
    website1_label = "Website1"
    website2_label = "Website2"
    line1_label = "Line 1"
    city_label = "City"
    state_abbr_label = "State Abbr"
    zip_code_label = "Zip Code"

    pandas.io.formats.excel.header_style = None
    log.info("Done.")
    
    max_len_headers = header_rows_df.applymap(lambda x: len(str(x))).max()
    max_len_data = data_rows_df.applymap(lambda x: len(str(x))).max()

    log.info("Writing to export file...")

    with pd.ExcelWriter(os.path.join(SCRIPT_PATH, filename), engine="xlsxwriter", engine_kwargs={"options": {"strings_to_urls": False}}) as writer:
        log.info("Creating workbook...")
        workbook = writer.book
        log.info("Done.")
        notes_format = workbook.add_format({'font_size': 12, 'align': 'top'})
        log.info("Creating worksheet - \"{zs}\"".format(zs="Office {oid}".format(oid=office_id)))
        log.info("Writing header rows...")
        header_rows_df.transpose().to_excel(writer, sheet_name="Office {oid}".format(oid=office_id), startrow=-1, startcol=2, header=False, index=False)
        worksheet = writer.sheets["Office {oid}".format(oid=office_id)]
        worksheet.write(0, 1, primary_network_label)
        worksheet.write(1, 1, franchise_network_label)  
        worksheet.write(2, 1, roster_url_label)  
        worksheet.write(3, 1, website1_label)  
        worksheet.write(4, 1, website2_label)  
        worksheet.write(5, 1, line1_label)  
        worksheet.write(6, 1, city_label)  
        worksheet.write(7, 1, state_abbr_label)  
        worksheet.write(8, 1, zip_code_label)  
        worksheet.merge_range("B10:B12", "Notes", notes_format)
        worksheet.merge_range("C10:G12", "", notes_format)
        log.info("Done.")
        log.info("Writing data rows...")
        data_rows_df.to_excel(writer, sheet_name="Office {oid}".format(oid=office_id), startrow=13, startcol=0, index=False)
        worksheet.autofilter("A14:O{rr}".format(rr=len(data_rows_df.index) + 14))
        worksheet.freeze_panes(14, 0)
        log.info("Done.")
        log.info("Adding formatting and column sizing...")
        new_format = workbook.add_format()
        new_format.set_font_size(12)
        new_format.set_align("left")

        col_len_min = 20

        for x in range(len(data_rows_df.columns)):
            col = chr(int(x) + 97).upper()
            col_name = data_rows_df.columns[x]
            col_size = max_len_data["{col}".format(col=col_name)]

            if x == 2:
                if col_size < max_len_headers.max():
                    col_size = max_len_headers.max()
            else:
                if col_size < col_len_min:
                    col_size = col_len_min     

            worksheet.set_column("{col}:{col}".format(col=col), col_size, new_format)
        
        log.info("Done.")

        log.info("Creating worksheet - \"{gr}\"".format(gr="Directions"))
        log.info("Adding directions info...")

        ## Add directions sheet
        A1 = "Step Number"
        A2 = 1
        A3 = 2
        A4 = 3
        A5 = 4
        A6 = 5
        A7 = 6
        B1 = "Step"
        B2 = "Open the link for Website1. If link is not provided, open Website2"
        B3 = "Locate on the website the page which displays the office's roster for the specific address show on the previous Excel sheet. Review the address to confirm the roster on the site matches the address of the office in the spreadsheet"
        B4 = "Add contacts to a new row on the spreadsheet that are on the website but not already on the spreadsheet, leaving the \"Contact ID\" column empty for the new row. Follow the column guide below"
        B5 = "Update row with contacts already entered into the spreadsheet, locate them on the office's roster on the website. When the contact is located, update the columns on the spreadsheet"
        B6 = "If the contact is not located on the website, place an \"X\" into the column named \"Not Found\", no further updating of the row is needed"
        B7 = "When file has been fully updated, press \"Show Email\" on the upper right portion of the Outlook window, followed by the \"Send\" button. This file has now been completed"
        B8 = ""
        B9 = "Column Guide"
        B10 = "contact_id"
        B11 = "office_id"
        B12 = "first_name"
        B13 = "last_name"
        B14 = "gender"
        B15 = "nickname"
        B16 = "title"
        B17 = "cell_phone"
        B18 = "direct_phone"
        B19 = "email"
        B20 = "photo_url"
        B21 = "linkedin_url"
        B22 = "facebook_url"
        B23 = "twitter_url"
        B24 = "zillow_url"
        B25 = "trulia_url"
        C1 = ""
        C2 = "If no website is provided or neither urls are functioning, make a note in the note section. Do not proceed with data collection"
        C3 = "If the specific roster for the address cannot be found, make a note in the note section. DO NOT PROCEED WITH DATA COLLECTION!"
        C4 = ""
        C5 = ""
        C6 = ""
        C7 = ""
        C8 = ""
        C9 = ""
        C10 = "Do not make changes. Contacts added to the sheet should remain empty. For internal use only"
        C11 = "Do not make changes. Contacts added to the sheet should remain empty. For internal use only"
        C12 = "Contact's first name. Existing rows should not be changed"
        C13 = "Contact's last name. Existing rows should not be changed"
        C14 = "Used to identify the contact's gender based on the photo on the website. If the gender already has been filled in with \"f\" or \"m\", do not make changes. If there are no pictures of the contacts on the website, leave this column unchanged and blank for new contacts you've entered into the spreadsheet"
        C15 = "The shortened or modified variation on the person's real name, as it appears on the website"
        C16 = "The business title the contacts has on the website. Examples would be \"Agent\", \"Owner\", \"Broker\". If no title exists, leave blank."
        C17 = "The cell phone as found on the website. If a cell phone exists on the spreadsheet but one cannot be found on the website, leave the existing cell phone unchanged. If a different cell phone number is found on the website, replace the existing data on the spreadsheet"
        C18 = "The direct phone as found on the website. This is a phone number different than the cell phone that would be used to reach them directly. If a direct phone exists on the spreadsheet but one cannot be found on the website, leave the existing direct phone unchanged. If a different direct phone number is found on the website, replace the existing data on the spreadsheet"
        C19 = "The contact's email address as found on the website. If an email address exists on the spreadsheet but one cannot be found on the website, leave the existing email address  unchanged. If a different email address  number is found on the website, replace the existing data on the spreadsheet"
        C20 = "The url of the image for the contact as found on the website. Update all rows in the spreadsheet, replacing existing data. If data exists on the spreadsheet but no photo url can be found on the website, leave the spreadsheet data unchanged"
        C21 = "The url of the contact's linkedin account as found on the website. If a linkedin account url exists on the spreadsheet but one cannot be found on the website, leave the existing linkedin account url unchanged. If a different linkedin account url number is found on the website, replace the existing data on the spreadsheet"
        C22 = "The url of the contact's facebook account as found on the website. If a facebook account url exists on the spreadsheet but one cannot be found on the website, leave the existing facebook account url unchanged. If a different facebook account url number is found on the website, replace the existing data on the spreadsheet"
        C23 = "The url of the contact's twitter account as found on the website. If a twitter account url exists on the spreadsheet but one cannot be found on the website, leave the existing twitter account url unchanged. If a different twitter account url number is found on the website, replace the existing data on the spreadsheet"
        C24 = "The url of the contact's zillow account as found on the website. If a zillow account url exists on the spreadsheet but one cannot be found on the website, leave the existing zillow account url unchanged. If a different zillow account url number is found on the website, replace the existing data on the spreadsheet"
        C25 = "The url of the contact's trulia account as found on the website. If a trulia account url exists on the spreadsheet but one cannot be found on the website, leave the existing trulia account url unchanged. If a different trulia account url number is found on the website, replace the existing data on the spreadsheet"

        a_format = workbook.add_format({'font_size': 12, 'align': 'center'})
        b_format = workbook.add_format({'font_size': 12, 'text_wrap': True})
        c_format = workbook.add_format({'font_size': 12, 'text_wrap': True})
        bold_format = workbook.add_format({'font_size': 12, 'bold': True})
        bad_style_format = workbook.add_format({'font_size': 12, 'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'text_wrap': True})

        worksheet2 = workbook.add_worksheet("Directions")
        worksheet2.write(0, 0, A1, bold_format)
        worksheet2.write(1, 0, A2)  
        worksheet2.write(2, 0, A3)  
        worksheet2.write(3, 0, A4)  
        worksheet2.write(4, 0, A5)  
        worksheet2.write(5, 0, A6)  
        worksheet2.write(6, 0, A7)  
        worksheet2.merge_range("B1:C1", B1, bold_format)
        worksheet2.write(1, 1, B2)  
        worksheet2.write(2, 1, B3)  
        worksheet2.write(3, 1, B4)  
        worksheet2.write(4, 1, B5)  
        worksheet2.write(5, 1, B6)  
        worksheet2.write(6, 1, B7)  
        worksheet2.write(8, 1, B9, bold_format)  
        worksheet2.write(9, 1, B10)  
        worksheet2.write(10, 1, B11)  
        worksheet2.write(11, 1, B12)  
        worksheet2.write(12, 1, B13)  
        worksheet2.write(13, 1, B14)  
        worksheet2.write(14, 1, B15)  
        worksheet2.write(15, 1, B16)  
        worksheet2.write(16, 1, B17)  
        worksheet2.write(17, 1, B18)  
        worksheet2.write(18, 1, B19)  
        worksheet2.write(19, 1, B20)  
        worksheet2.write(20, 1, B21)  
        worksheet2.write(21, 1, B22)  
        worksheet2.write(22, 1, B23)  
        worksheet2.write(23, 1, B24)  
        worksheet2.write(24, 1, B25)  
        worksheet2.write(1, 2, C2)  
        worksheet2.write(2, 2, C3, bad_style_format)    
        worksheet2.write(9, 2, C10, bad_style_format)  
        worksheet2.write(10, 2, C11, bad_style_format)  
        worksheet2.write(11, 2, C12)  
        worksheet2.write(12, 2, C13)  
        worksheet2.write(13, 2, C14)  
        worksheet2.write(14, 2, C15)  
        worksheet2.write(15, 2, C16)  
        worksheet2.write(16, 2, C17)  
        worksheet2.write(17, 2, C18)  
        worksheet2.write(18, 2, C19)  
        worksheet2.write(19, 2, C20)  
        worksheet2.write(20, 2, C21)  
        worksheet2.write(21, 2, C22)  
        worksheet2.write(22, 2, C23)  
        worksheet2.write(23, 2, C24)  
        worksheet2.write(24, 2, C25)  
        log.info("Done.")
        log.info("Adding formatting and column sizing...")

        worksheet2.set_column("A:A", 15, a_format)
        worksheet2.set_column("B:B", 80, b_format)
        worksheet2.set_column("C:C", 130, c_format)
        log.info("Done.")

    log.info("Preparing email, getting attachments...")
    # Add attachments
    if FULL_DRY_RUN:
        log.info("DRY_RUN: skipping PDF attachment")
        file_list = [filename]
    else:
        file_list = ["Clean Office Roster.pdf", filename]

    attachments = []

    for file_x in file_list:
        log.info("Attaching file: {fx}".format(fx=file_x))
        target_file = os.path.join(SCRIPT_PATH, file_x)
        if os.path.exists(target_file):
            with open(target_file, 'rb') as f:
                content = f.read()
            attachments.append((file_x, content))
            log.info("Done.")
        else:
            log.critical("File not found!")
            raise IOError("File '{sa}' not found!".format(sa=target_file))

    # Send email
    log.info("Sending email to \"{ea}\" for Office ID {oid}".format(ea=email_address, oid=office_id))
    send_email(account, "ReminderMedia: Clean Office Roster for Office {oid}".format(oid=office_id),"Please see the attached \"Clean Office Roster.pdf\" file for instructions.", email_address,attachments=attachments)

    ## Update stats table
    params = [office_id, workflow_id, len(office_roster_full_df.index), email_address, filename]
    update_stats_table(params)

    ## Set roster status
    log.info(f'Workflow ID {workflow_id} data type is {type(workflow_id)}')
    #check if 'workflow_id' is not an integer
    if not isinstance(workflow_id, int):
            try:
                # Attempt to retrieve the integer value if 'workflow_id' is numpy datatype or similar
                workflow_id = workflow_id.item()
            except AttributeError:
                # If .item() method doesn't exist, attempt a direct integer conversion
                try:
                    workflow_id = int(workflow_id)
                except (ValueError, TypeError):
                    # Handle the case where conversion fails
                    err = "Error: workflow_id is not convertible to int"
                    send_teams_message_safe(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                                            activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=err)
                    
                    # Handle error accordingly (e.g., set to None or handle exception)
                    workflow_id = None  # or raise an exception, or whatever your error handling strategy is

    log.info(f'Workflow ID {workflow_id} data type is now {type(workflow_id)}')
    set_roster_status(workflow_id)

def get_file_mtime(file_path):
    """
    Get the last modified timestamp for a file
    """
    # Verified that moving does not affect the file mtime
    if os.path.isfile(file_path):
        file_time = os.path.getmtime(file_path)

        return int(file_time)

def calculate_cutoff(num_days=1):
    """
    Calculate cutoff time for file age
    """
    str_cutoff = datetime.datetime.strftime((datetime.date.today() - datetime.timedelta(days=num_days)), "%m/%d/%Y")
    date_cutoff = datetime.datetime.strptime((str_cutoff) + " 11:59:59", "%m/%d/%Y %H:%M:%S")
    cutoff_timestamp = int(time.mktime(date_cutoff.timetuple()))

    return cutoff_timestamp

def purge_archives(target_directory, cutoff_days):
    """
    Build a list of files that have expired from archive storage and delete those files
    """
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    log.info("Checking for archive files to purge...")

    removed_files = 0
    cutoff_time = calculate_cutoff(cutoff_days)
    files = os.listdir(target_directory)
    for xfile in files:
        target_file = os.path.join(target_directory, xfile)
        if os.path.isfile(target_file):
                file_mtime = get_file_mtime(target_file)
                # Delete files from target directory if older than a certain age
                if file_mtime <= cutoff_time:
                    os.remove(target_file)
                    removed_files += 1

    log.info("Files removed: {rf}".format(rf=removed_files))

def backup_files():
    import glob
    import zipfile

    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    backup_loc = os.path.join(SCRIPT_PATH, BACKUP_DIR)

    log.info("Checking for backup directory ({gg})...".format(gg=backup_loc))
    if not os.path.exists(backup_loc):
        log.info("Backup path not found, creating...")
        os.mkdir(backup_loc)
        log.info("Done.")
    else:
        log.info("Backup path found.")

    ## Add all xlsx files to zip file and move to backup dir
    file_list = glob.glob(os.path.join(SCRIPT_PATH, "*.xlsx"))
    log.info("{rf} files found to backup: {ws}".format(rf=len(file_list), ws=file_list))

    zip_name = os.path.join(backup_loc, "backup_{dt}.zip".format(dt=datetime.datetime.today().strftime("%Y_%m_%d")))

    log.info("Creating zip archive: {za}".format(za=zip_name))

    with zipfile.ZipFile(zip_name, "a") as zipf: 
        # write each file one by one 
        for backup_file in file_list:
            log.info("Adding file to zip archive: {qs}".format(qs=backup_file)) 
            zipf.write(backup_file) 
    
    log.info("Cleaning up...")
    for backup_file in file_list:
        log.info("Deleting file: {zz}".format(zz=backup_file))
        os.remove(os.path.join(SCRIPT_PATH, backup_file))

def read_google_sheet():
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    log.info("Getting Google Sheet data...")
    ## Use creds to create a client to interact with the Google Drive API
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    log.info("Loading credentials.")
    creds = ServiceAccountCredentials.from_json_keyfile_name(os.path.join(SCRIPT_PATH + "credentials.json"), scope)
    client = gspread.authorize(creds)
    log.info("Client loaded.")

    # Find a workbook by name and open the first sheet
    # Make sure you use the right name here.
    log.info("Opening sheet.")
    workbook = client.open("Clean Office Roster Export")
    sheet = workbook.worksheet("Sheet1")

    # Extract and print all of the values
    log.info("Getting sheet data.")
    sheet_data = sheet.get_all_records()
    log.info("Saving sheet data.")  ## Let's save a copy of the Google Sheet data in case of I/O issues we can use the last saved copy
    gspread_df = pd.DataFrame(sheet_data)
    gspread_df.to_json(GOOGLE_SHEET_DATA_FILE, orient="records")
    log.info("Google Sheet Data: {gsd}".format(gsd=gspread_df.to_json(orient="records")))
    log.info("Sheet data saved.")

    return gspread_df

def load_google_sheets_data():
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    for i in range(1, RETRY_ATTEMPTS + 1):
        try:
            ## Get Google Sheet with caller info
            google_data_df = read_google_sheet()
        except (gspread.exceptions.GSpreadException) as e:
            log.error("Exception: {e}".format(e=repr(e)))
            if i < (RETRY_ATTEMPTS + 1):
                log.error("Google Sheet unavailable, pausing {retry_pause} seconds and retrying...  Attempt {i}".format(retry_pause=RETRY_PAUSE, i=i))
            else:
                log.error("Google Sheet unavailable, pausing {retry_pause} seconds and retrying...  Attempt {i} - Last attempt.".format(retry_pause=RETRY_PAUSE, i=i))
            time.sleep(RETRY_PAUSE)
        else:
            break
    else:
        ## All attempts failed - deal with the consequences.
        log.error("All attempts failed.  Sending alert and using previous data.")
        ## TODO: Send alert
        if os.path.exists(GOOGLE_SHEET_DATA_FILE):
            google_data_df = pd.read_json(GOOGLE_SHEET_DATA_FILE)
        else:
            raise IOError("Google Sheet data file not found at: {gsd}".format(gsd=GOOGLE_SHEET_DATA_FILE))

    ## Replace any missing values with 0 for Send Amount
    google_data_df["Send Amount"] = google_data_df["Send Amount"].replace(r"", 0, regex=True)

    google_data_len = len(google_data_df.index)
    
    log.info("Creating Google Sheets DataFrame.")
    log.info("Records returned: {rr}".format(rr=google_data_len))

    return google_data_df


def main():
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    try:
        # export_office_roster(35678, 7616997, "nobody@example.com")
        ## read in Google Sheet
        if FULL_DRY_RUN:
            log.info("DRY_RUN: using synthetic Google Sheet data")
            google_df = pd.DataFrame(
                [{
                    "Email Address": "dryrun@example.com",
                    "Send Amount": 3
                }]
            )
            has_google_rows = len(google_df.index) > 0
        else:
            google_df = load_google_sheets_data()
            has_google_rows = len(google_df.index) > 0
        if has_google_rows:
            # Fetch data and create the combined dataframe outside the recipient loop
            office_webinar_list_df = grab_office_list_just_webinar()
            office_webinar_list_df['office_id'] = office_webinar_list_df['office_id'].astype(int)
            office_webinar_list_df["host_office_id"] = pd.NA
            office_webinar_list_df["relationship_type"] = pd.NA
            log.info(office_webinar_list_df)
            print(office_webinar_list_df)

            office_meets_at_list_df = grab_office_list_with_meets_at_relationship()
            office_meets_at_list_df['office_id'] = office_meets_at_list_df['office_id'].astype(int)
            log.info(office_meets_at_list_df)

            office_list = get_office_list()
            office_list['office_id'] = office_list['office_id'].astype(int)
            office_list["host_office_id"] = pd.NA
            office_list["relationship_type"] = pd.NA
            log.info(office_list)

            office_advocate_list = get_advocate_relationship()
            office_advocate_list['office_id'] = office_advocate_list['office_id'].astype(int)
            log.info(office_advocate_list)

            combined_df_total = pd.concat([office_webinar_list_df, office_meets_at_list_df, office_advocate_list, office_list], ignore_index=True)
            combined_df_total = combined_df_total.drop_duplicates(subset=['workflow_id', 'office_id'], ignore_index=True)

            total_send_amount = google_df["Send Amount"].sum()
            log.info("Total send amount is {tsm}".format(tsm=total_send_amount))

            missing_required = combined_df_total["office_id"].isna() | combined_df_total["workflow_id"].isna()
            if missing_required.any():
                missing_rows = combined_df_total[missing_required]
                for _, row in missing_rows.iterrows():
                    log.warning(
                        "Excluding candidate due to missing data: office_id={oid}, workflow_id={wid}".format(
                            oid=row.get("office_id"),
                            wid=row.get("workflow_id")
                        )
                    )
                combined_df_total = combined_df_total.loc[~missing_required].copy()

            combined_df_total["activity_created_at"] = pd.to_datetime(
                combined_df_total["activity_created_at"], errors="coerce"
            )
            combined_df_total["host_office_id"] = pd.to_numeric(
                combined_df_total["host_office_id"], errors="coerce"
            )

            candidate_office_ids = combined_df_total["office_id"].dropna().astype(int).tolist()
            candidate_host_ids = combined_df_total["host_office_id"].dropna().astype(int).tolist()
            webinar_info_df = get_current_webinar_info(candidate_office_ids + candidate_host_ids)
            webinar_info_df["webinar_date"] = pd.to_datetime(
                webinar_info_df["webinar_date"], errors="coerce"
            )
            webinar_dates = webinar_info_df.set_index("office_id")["webinar_date"].to_dict()
            webinar_last_updated = webinar_info_df.set_index("office_id")[
                "opportunity_last_updated"
            ].to_dict()

            combined_df_total["webinar_date"] = combined_df_total["office_id"].map(webinar_dates)
            combined_df_total["host_webinar_date"] = combined_df_total["host_office_id"].map(webinar_dates)
            combined_df_total["opportunity_last_updated"] = combined_df_total["office_id"].map(
                webinar_last_updated
            )
            combined_df_total["host_opportunity_last_updated"] = combined_df_total["host_office_id"].map(
                webinar_last_updated
            )

            # Jacksonville remediation: re-evaluate tiering every run with current
            # webinar dates to catch newly scheduled webinars after activity creation.
            combined_df_total["tier"] = 3
            combined_df_total["controlling_date"] = combined_df_total["activity_created_at"]
            tier1_mask = combined_df_total["webinar_date"].notna()
            combined_df_total.loc[tier1_mask, "tier"] = 1
            combined_df_total.loc[tier1_mask, "controlling_date"] = combined_df_total.loc[
                tier1_mask, "webinar_date"
            ]
            tier2_mask = (~tier1_mask) & combined_df_total["host_webinar_date"].notna()
            combined_df_total.loc[tier2_mask, "tier"] = 2
            combined_df_total.loc[tier2_mask, "controlling_date"] = combined_df_total.loc[
                tier2_mask, "host_webinar_date"
            ]
            combined_df_total["opportunity_last_updated"] = combined_df_total[
                "opportunity_last_updated"
            ].where(
                combined_df_total["tier"] != 2,
                combined_df_total["host_opportunity_last_updated"]
            )

            combined_df_total = combined_df_total.sort_values(
                by=["tier", "controlling_date"],
                ascending=[True, True],
                na_position="last"
            ).reset_index(drop=True)

            tier_counts = combined_df_total["tier"].value_counts().to_dict()
            log.info("Roster candidates: {count}".format(count=len(combined_df_total.index)))
            log.info(
                "Tier counts: tier_1={t1}, tier_2={t2}, tier_3={t3}".format(
                    t1=tier_counts.get(1, 0),
                    t2=tier_counts.get(2, 0),
                    t3=tier_counts.get(3, 0)
                )
            )

            # Limit the number of rows in combined_df based on total_send_amount
            combined_df = combined_df_total.head(total_send_amount)
            log.info("Selected roster count: {count}".format(count=len(combined_df.index)))
            log.info("Top 20 roster prioritization decisions:")
            for _, row in combined_df.head(20).iterrows():
                log.info(
                    "office_id={oid} workflow_id={wid} tier={tier} "
                    "controlling_date={cd} relationship_type={rt} "
                    "opportunity_last_updated={olu}".format(
                        oid=row.get("office_id"),
                        wid=row.get("workflow_id"),
                        tier=row.get("tier"),
                        cd=row.get("controlling_date"),
                        rt=row.get("relationship_type"),
                        olu=row.get("opportunity_last_updated")
                    )
                )
            for idx, row in google_df.iterrows():
                email_address = str(row["Email Address"])
                send_amount = int(row["Send Amount"])
                if email_address:
                    if send_amount > 0:
                        log.info(f"Send amount requested: {send_amount} | Offices found by query: {len(combined_df)}")
                        sent_count = 0  # Initialize sent count for this email_address
                        # Create a copy of the indices to iterate and drop rows
                        indices_to_process = combined_df.index.tolist()
                        for index in indices_to_process:
                            if sent_count >= send_amount:
                                break  # Exit the loop if we have reached the send limit
                            office = combined_df.loc[index]
                            if not isinstance(office["workflow_id"], int):
                                try:
                                    office["workflow_id"] = int(office["workflow_id"])
                                    log.info(type(office["workflow_id"]))
                                except ValueError:
                                    err = f"Cannot convert workflow_id {office['workflow_id']} to int."
                                    log.error(err)
                                    send_teams_message_safe(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                                                            activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=err)
                                    
                                    raise  # Re-throw the caught exception to terminate the script

                            if not isinstance(office["office_id"], int):
                                try:
                                    office["office_id"] = int(office["office_id"])
                                    log.info(type(office["office_id"]))
                                except ValueError:
                                    err = f"Cannot convert office_id {office['office_id']} to int."
                                    log.error(err)
                                    send_teams_message_safe(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                                                            activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=err)
                                    
                                    raise  # Re-throw the caught exception to terminate the script

                            log.info("Office ID being sent is {oid} and corresponding workflow ID is {wid}".format(oid=office["office_id"],wid=office["workflow_id"]))
                            export_office_roster(office["office_id"], office["workflow_id"], email_address)
                            sent_count += 1
                            log.info(f"Send Count for {email_address} is {sent_count}")
                            # Drop the row from the dataframe
                            combined_df.drop(index, inplace=True)
                    else:
                        log.error(f"Send amount is zero for {email_address}!")
                else:
                    log.error(f"Blank email found at index position {idx}")
            if FULL_DRY_RUN:
                log.info("DRY_RUN: would back up files")
            else:
                backup_files()
        else:
            err = "No rows in Google Sheet!"
            log.critical(err)
            raise ValueError(err)

    except Exception as e:
        log.critical("Critical error has occurred, sounding alarm!  Error info: {e}".format(e=e))
        err = (traceback.print_exc())
        print(err)
        send_teams_message_safe(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                                activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=err)
        
    finally:
        log.info("Closing database connection...")
        db.close()
        log.info("Database connection closed.")
        end_ts = time.time()
        final_time = end_ts - start_ts
        log.info("Processing finished.  Time: {mt} minutes ({et} seconds)".format(mt=round(final_time / 60, 3), et=round(final_time, 3)))
        log.info("-" * 120)

if __name__ == '__main__':
    main()
