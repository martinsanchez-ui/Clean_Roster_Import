####################################################################################################
#	Author: Chris O'Brien   
#	Created On: Mon Apr 27 2020
#	File: office_roster_input.py
#	Description: 
####################################################################################################
import datetime
import os
import sys
import time
import traceback
import warnings

import MySQLdb as mysql
import pandas as pd
import requests
import unidecode
from dotenv import load_dotenv
from exchangelib import Account, Configuration, FileAttachment, Identity, IMPERSONATION, OAuth2Credentials
from exchangelib.errors import ErrorIrresolvableConflict
from exchangelib.version import EXCHANGE_O365, Version
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from etl_utilities.RMLogger import Logger
from etl_utilities.utils import send_teams_message, load_secrets


load_dotenv()

SCRIPT_PATH = sys.path[0] + os.sep
ENV_FILE_PATH = os.path.join(os.path.sep, "datafiles", "app_credentials.env")
LOG_LOCATION = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "log.log")
SEND_TEAMS_MESSAGE_SUMMARY = "Clean_Roster_Import"
SEND_TEAMS_MESSAGE_ACTIVITY_TITLE = "Error occurred - Clean_Roster_Import"
WAR_WEBHOOK_KEY = "warning_send_teams_message_url"
DEV_WEBHOOK_KEY = "dev_send_teams_message_url"
ETL_ENV = os.getenv("ETL_ENV")
WEBHOOK_KEY_TO_USE = WAR_WEBHOOK_KEY if ETL_ENV == "prod" else DEV_WEBHOOK_KEY
EMAIL_SUBJECT = "ReminderMedia: Clean Office Roster for Office "
EMAIL_LOOKBACK_DAYS = 1
BACKUP_DIR = os.path.join(SCRIPT_PATH, "backup")
ERROR_DIR = os.path.join(SCRIPT_PATH, "error")



SECRETS = load_secrets(ENV_FILE_PATH,
                       ["dbmaster_hostname", "dbmaster_username", "dbmaster_password", "dbmaster_database", 
                        "bi_hostname", "bi_username", "bi_password", "bi_database",
                        "client_id", "client_secret", "tenant_id", "primary_smtp_address", "outlook_server", 
                        "delete_contact_api_url", "delete_contact_api_key", "update_case_status_api_url", "update_case_status_api_key"])

log = Logger(log_file_location=LOG_LOCATION, log_file_backup_count=50, logging_level="DEBUG")

is_python3 = sys.version_info.major == 3
if is_python3:
    unicode = str

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
pd.options.display.width = 0
warnings.filterwarnings("ignore", category=mysql.Warning)


def mark_read_with_retry(msg, retries=3, backoff=0.5):
    for i in range(retries):
        try:
            msg.refresh()
            msg.is_read = True
            msg.save(update_fields=['is_read'])
            return
        except ErrorIrresolvableConflict:
            if i == retries - 1:
                raise
            time.sleep(backoff * (2 ** i))

#TODO: Replace this with an initialization function like the one in "Indeed_Automation"
credentials = OAuth2Credentials(client_id=SECRETS.get('client_id'), client_secret=SECRETS.get('client_secret'), tenant_id=SECRETS.get('tenant_id'),
                                identity=Identity(primary_smtp_address=SECRETS.get('primary_smtp_address')))
config = Configuration(credentials=credentials, server=SECRETS.get('outlook_server'), version=Version(build=EXCHANGE_O365))
account =  Account(primary_smtp_address=SECRETS.get('primary_smtp_address'), config=config, autodiscover=False, access_type=IMPERSONATION)

log.info("Opening database connection...")
dbmaster = mysql.connect(SECRETS.get("dbmaster_hostname"), SECRETS.get("dbmaster_username"), SECRETS.get("dbmaster_password"), SECRETS.get("dbmaster_database"))
bi = mysql.connect(SECRETS.get("bi_hostname"), SECRETS.get("bi_username"), SECRETS.get("bi_password"), SECRETS.get("bi_database"))
log.info("Database connection open.")

#TODO: THIS FUNCTION IS USED IN MULTIPLE SCRIPTS. IT SHOULD BE MOVED TO ETL_UTILITIES. ALSO, IT SHOULD BE REFACTORED.
def execute_query(db, sql, params=None, db_name=None):
    max_retries = 3
    try:
        log.info('Connecting to database')
        log.debug(db)
        log.debug(f"SQL: {sql}")
        log.debug(f"Params: {params}")
        cursor = db.cursor()
        cursor.execute(sql, params)
        return cursor  # Return the cursor object
    except mysql.OperationalError as e:
        try:
            for x in range(max_retries):
                try:
                    # x = a if condition1 else (b if condition2 else c)
                    db_hostname = SECRETS.get("dbmaster_hostname") if db_name == "CRM" else (SECRETS.get("bi_hostname") if db_name == "BI" else "")
                    log.debug(db_hostname)
                    db_username = SECRETS.get("dbmaster_username") if db_name == "CRM" else (SECRETS.get("bi_username") if db_name == "BI" else "")
                    db_password = SECRETS.get("dbmaster_password") if db_name == "CRM" else (SECRETS.get("bi_password") if db_name == "BI" else "")
                    db_database = SECRETS.get("dbmaster_database") if db_name == "CRM" else (SECRETS.get("bi_database") if db_name == "BI" else "")
                    log.debug(db_database)
                    
                    db = mysql.connect(db_hostname, db_username, db_password, db_database)
                    cursor = db.cursor()
                    cursor.execute(sql, params)

                    log.info(f"SQL: {sql}")
                    log.info(f"Params: {params}")
                    return cursor  # Return the cursor object
                except:
                    log.warning('Try number {x} failed. Trying again in 2 seconds'.format(x))
                    time.sleep(2)
            err = 'Max retries attempts failed when executing SQL query: {sql} {db_hostname}, {db_database}'.format(
                sql=sql, db_hostname=db_hostname, db_database=db_database)
            log.error(err)
            send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                               activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=err)

        except Exception as e:
            # Re-raise any other exception
            send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                               activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=e)
            print(e)
            log.warning('Retries reached')
            pass


def already_processed(bi_conn, office_id, received_filename):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    select_sql = """
    SELECT
        1
    FROM
        bi_warehouse_prd.office_roster_automation_f
    WHERE
        office_id = %s
        AND received_filename = %s
    LIMIT 1;
    """
    lookup_cursor = execute_query(
        bi_conn,
        select_sql,
        (office_id, received_filename),
        db_name="BI"
    )
    return lookup_cursor.fetchone() is not None


def update_companies_table(oid, emp_cnt):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    log.info("Updating xrms.companies for company_id: {oid}".format(oid=oid))

    update_sql = "UPDATE xrms.companies SET employees=%s WHERE company_id=%s"
    try:
        execute_query(dbmaster, update_sql, [emp_cnt, oid], db_name='CRM')
        dbmaster.commit()
        log.info("Update successful.")
    except Exception as e:
        log.error("Update failed with error: {e}".format(e=e))
        log.error(traceback.format_exc())


def clean_phone_number(phone_number):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    if isinstance(phone_number, str):
        phone_number = phone_number.encode("utf-8").decode("ascii", "ignore")
        # phone_number = phone_number.encode("utf-8").decode("ascii", "ignore")

    if ".0" in str(phone_number)[-2:]:
        ## Fix the Pandas 'long' issue
        phone_number = str(int(phone_number))

    clean_phone = "".join(c for c in str(phone_number) if c.isdigit())
    if len(clean_phone.strip()) > 0:
        return (str(clean_phone))
    else:
        return ""


def filter_non_numeric(target):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    if ".0" in str(target)[-2:]:
        ## Fix the Pandas 'long' issue
        target = str(int(target))

    cleaned = "".join(c for c in str(target) if c.isdigit())
    if len(cleaned.strip()) > 0:
        return (str(cleaned))
    else:
        return 0


def get_contact_info(user_id):
    """ Returns a list with the contact info per user """

    SQL_QUERY = """
        SELECT 
            cci.id,
            cci.contacttype,
            cci.contactinfo
            
        FROM
            xrms.contacts_contactinfo cci
            JOIN xrms.contacts c ON (cci.userid=c.contact_id)
            
        WHERE
            cci.userid = %s
            AND cci.row_status = 'a'
            AND c.contact_record_status = 'a'
    """

    try:
        lookup_cursor = execute_query(dbmaster, SQL_QUERY, (user_id,), db_name="CRM")
        result = lookup_cursor.fetchall()
        return result
        
    except Exception as e:
        log.error(f"An error occurred in get_contact_info() : {e}")
        return None


def check_db_for_user(contactinfo, company_id, is_email=False, contact_id=0):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    lookup_user_sql = """
        SELECT 
            cci.userid
        FROM
            xrms.contacts_contactinfo cci
            JOIN xrms.contacts c ON (cci.userid=c.contact_id)
        WHERE
            cci.contactinfo = %s
            AND cci.row_status = 'a'
            AND c.contact_record_status = 'a'
            AND c.company_id = %s
    """

    if is_email:
        lookup_user_sql += " AND cci.discriminator = 'Email'"
    else:
        lookup_user_sql += " AND cci.discriminator = 'Phone'"

    if contact_id:
        lookup_user_sql += f" AND cci.userid = {contact_id}"
    
    lookup_user_sql += " LIMIT 1;"

    try:
        lookup_cursor = execute_query(dbmaster, lookup_user_sql, (contactinfo, company_id), db_name="CRM")
        contact_id = lookup_cursor.fetchone()

        if contact_id is not None:
            return contact_id[0]
        else:
            return 0
    except Exception as e:
        log.error(f"An error occurred while looking up the user: {e}")
        return 0


def check_db_for_user_insert(contactinfo, company_id, is_email=False):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    lookup_user_sql = """
        SELECT 
            cci.contacttype,
            cci.contactinfo,
            cci.userid,
            c.company_id,
            c.first_names,
            c.last_name
        FROM
            xrms.contacts_contactinfo cci
            JOIN xrms.contacts c ON (cci.userid=c.contact_id)
        WHERE
            cci.contactinfo = %s
            AND cci.row_status = 'a'
            AND c.contact_record_status = 'a'
            AND c.company_id = %s
    """

    # Add discriminator condition based on is_email flag
    if is_email:
        lookup_user_sql += " AND cci.discriminator = 'Email'"
    else:
        lookup_user_sql += " AND cci.discriminator = 'Phone'"

    try:
        # Execute first query
        lookup_cursor = execute_query(dbmaster, lookup_user_sql, (contactinfo, company_id), db_name="CRM")
        contact_info = lookup_cursor.fetchall()

        # Convert the result to a DataFrame
        df = pd.DataFrame(contact_info,
                          columns=['contacttype', 'contactinfo', 'userid', 'company_id', 'first_name', 'last_name'])

        return df

    except Exception as e:
        log.error(f"An error occurred while looking up the user: {e}")
        return None


def customer_check(contact_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    customer_check_sql = """SELECT 
        cd.contact_type_category_id 
    FROM 
        bi_warehouse_prd.contact_d cd 
    WHERE
        cd.contact_id = %s;"""

    try:
        customer_check_cur = execute_query(bi, customer_check_sql, (contact_id,), db_name="BI")
        result = customer_check_cur.fetchone()

        if result is not None:
            return result[0] == 2
        else:
            return False

    except Exception as e:
        log.error(f"An error occurred while checking the customer: {e}")
        return False


def open_workflows_check(contact_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    open_workflows_sql = """SELECT
       c.contact_id
    FROM
       xrms.cases c
    WHERE
       c.contact_id=%s
       AND c.case_record_status='a'
       AND (c.closed_at=0 OR c.closed_at IS NULL)
    UNION ALL  
    SELECT
       o.contact_id
    FROM
       xrms.opportunities o
    WHERE
       o.contact_id=%s
       AND o.opportunity_record_status='a'
       AND (o.closed_at=0 OR o.closed_at IS NULL);"""

    try:
        check_workflows_cur = execute_query(dbmaster, open_workflows_sql, [contact_id, contact_id], db_name="CRM")
        contact_id_result = check_workflows_cur.fetchone()

        if contact_id_result is not None:
            return True
        else:
            return False
    except Exception as e:
        error_message = f"An error occurred while checking for open workflows: {e}"
        log.error(error_message)
        send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=error_message)
        return False


def check_relationship_set(contact_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    relationship_sql = """
        SELECT 
            r.relationship_id 
        FROM 
            xrms.relationships r 
        WHERE 
            r.from_id = %s
            AND r.type_id IN (6, 10, 19, 24, 43, 68, 75, 83, 93, 107, 110) 
            AND r.ended_on = 0
    """

    try:
        check_relationship_cur = execute_query(dbmaster, relationship_sql, [contact_id], db_name="CRM")
        relationship_id = check_relationship_cur.fetchone()
        log.info(f'Relationship ID: {relationship_id}')

        if relationship_id is not None:
            return True
        else:
            return False
    except Exception as e:
        error_message = f"An error occurred while checking for relationship set: {e}"
        log.error(error_message)
        send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=error_message)
        
        return False


def check_if_deleted(db, contact_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    is_del_sql = """SELECT
    c.contact_record_status
FROM
    xrms.contacts c
WHERE
    c.contact_id = %s;"""

    try:
        is_del_cur = execute_query(db, is_del_sql, [contact_id], db_name="CRM")
        is_deleted = is_del_cur.fetchone()

        if is_deleted is not None:
            if is_deleted[0] == "a":  # Access the first element of the tuple
                return False
            else:
                return True
        else:
            return True

    except Exception as e:
        log.error(f"An error occurred while checking if the contact is deleted: {e}")
        return True


def delete_contact(contact_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    log.info("Deleting Contact ID: {cid}".format(cid=contact_id))

    params = {
        "api_key": SECRETS.get("delete_contact_api_key"),
        "contact_id": contact_id
    }

    try:
        log.info("API parameters: {p}".format(p=params))
        r = requests.post(SECRETS.get("delete_contact_api_url"), json=params, verify=False)
        log.info("API status code: {sc}".format(sc=r.status_code))
        log.info("API response: {ar}".format(ar=r.text))

    except Exception as e:
        log.critical("Error calling Contact Delete API!  Error: {e}".format(e=e))
        log.error(traceback.format_exc())


def contactinfo_insert(contact_id, contact_info, ctype, source_column=None):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    cci_insert_sql = """INSERT INTO xrms.contacts_contactinfo
(userid, contacttype, contactinfo, display_version, primaryemail, secondaryemail, contactinfo_opt_in_id, primarycontact, custlabel, lastupdate, row_status, import_id, discriminator, marketo_primary_contact)
VALUES(%s, %s, %s, '', %s, 'no', -1, %s, %s, current_timestamp(), 'a', 0, %s, 0);"""

    if source_column == "cell_phone":
        contacttype = "Cell"
        primaryemail = "no"
        primarycontact = "yes"
        custlabel = "Cell"
        discriminator = "Phone"
    elif ctype.lower() == "c":
        contacttype = "Cell"
        primaryemail = "no"
        primarycontact = "yes"
        custlabel = "Cell"
        discriminator = "Phone"
    elif ctype.lower() == "d":
        contacttype = "Direct"
        primaryemail = "no"
        primarycontact = "no"
        custlabel = "Direct"
        discriminator = "Phone"
    elif ctype.lower() == "e":
        contacttype = "Email"
        primaryemail = "yes"
        primarycontact = "no"
        custlabel = "E-mail"
        discriminator = "Email"
    else:
        log.critical("Invalid contact info type!")
        raise RuntimeError("Invalid contact info type!")

    cci_insert_params = (contact_id, contacttype, contact_info, primaryemail, primarycontact, custlabel, discriminator)

    try:
        cci_insert_cur = execute_query(dbmaster, cci_insert_sql, cci_insert_params, db_name="CRM")
        dbmaster.commit()
        cci_insert_id = cci_insert_cur.lastrowid
        log.info("Insert successful.  Inserted ID: {nc}".format(nc=cci_insert_id))
    except mysql.Error as e:
        log.error("Insert failed with MySQL error: {e}".format(e=e))
        log.error(traceback.format_exc())


def process_not_found(not_found_df):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    for idx, row in not_found_df.iterrows():
        log.info("Processing not found record: {iu}".format(iu=idx + 1))
        contact_id = row["contact_id"]
        log.info("Checking status of contact ID: {cid}".format(cid=contact_id))
        can_delete = False

        if contact_id > 0:
            log.info("Checking customer status...")
            if not customer_check(contact_id):
                log.info("The contact is not a customer.")
                log.info("Checking for any relationship sets...")
                if not check_relationship_set(contact_id):
                    log.info("The contact is not part of a relationship")
                    if not open_workflows_check(contact_id):
                        log.info("No open workflows found.")
                        can_delete = True
                    else:
                        log.info("Open workflow(s) found!  Skipping this contact.")
                else:
                    log.info("Open relationship found!  Skipping this contact.")
            else:
                log.info("This contact is a customer!  Skipping.")

            if can_delete:
                log.info("Preparing to delete contact ID: {cid}".format(cid=contact_id))
                delete_contact(contact_id)
        else:
            log.error("Invalid contact ID: {cid}".format(cid=contact_id))


def _adjust_contact_info(contact_info_list_db, contact_id, ci, ct):
    """ Checks if a provided contact info is stored in the db under the provided contact type
        and adjust it if required. 

        contact_info_list_db: List of contact info for a particular user found in the db
        ci: Phone number from the vendor's excel sheet
        ct: ["Direct"|"Cell"] Contact type as per vendor's excel sheet
    """
    
    IDX_ID = 0
    IDX_CONTACT_TYPE = 1
    IDX_CONTACT_INFO = 2

    ci_db_list = [x for x in contact_info_list_db if x[IDX_CONTACT_INFO] == ci]   ## WHAT ABOUT REPEATED PHONE NUMBERS?
    ci_db = ci_db_list[0] if ci_db_list else None
    ct_text = "Cell phone" if ct == "Cell" else "Direct phone"

    if ci_db:
        if ci_db[IDX_CONTACT_TYPE] == ct:
            log.info(f"{ct_text} {ci_db[IDX_CONTACT_INFO]} found in DB. No changes required.")
        else:
            log.info(f"Updating contact type for {ci}. Old: {ci_db[IDX_CONTACT_TYPE]}. New: {ct}]")
            _perform_contact_type_update(ci_db[IDX_ID], ct)
    else:
        log.info(f"Adding new {ct_text} phone: {ci}")
        ctype = "c" if ct == "Cell" else "d"  # c = Cell d = Direct
        source_column = "cell_phone" if ct == "Cell" else None
        contactinfo_insert(contact_id, ci, ctype=ctype, source_column=source_column)

def _perform_contact_type_update(id, contact_type):
    """ Updates contact types in xrms.contacts_contactinfo"""

    # Direct phones are stored with Contact type = "Phone"
    SQL_UPDATE = """
                    UPDATE 
                        xrms.contacts_contactinfo 
                    SET
                        contacttype = %s
                    WHERE
                        id = %s;
                """

    update_params = (contact_type, id)
    execute_query(dbmaster, SQL_UPDATE, update_params, db_name="CRM")
    dbmaster.commit()
    log.info(f"Record id: {id} was updated to contact type: {contact_type}")    

def process_update(update_df, company_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    for idx, row in update_df.iterrows():
        log.info("Processing update record: {iu}".format(iu=idx + 1))
        contact_id = int(row["contact_id"])
        has_open_opp = False
        is_in_program = False

        #TODO: I DON'T UNDERSTAND WHY row["gender"] != 0. THIS IS A BUG. THIS COMPARISON WILL NEVER WORK. 
        log.info("Checking for open opportunities on Contact ID: {cid}".format(cid=contact_id))
        if check_for_open_opps(contact_id) > 0:
            log.info("At least one open opportunity found.")
            has_open_opp = True
        else:
            log.info("No open opportunities found.")

        log.info("Checking if is customer on Contact ID: {cid}".format(cid=contact_id))
        
        if customer_check(contact_id):
            log.info("Contact is a customer.")
            is_in_program = True
        else:
            log.info("Contact is not a customer")

        gender = row["gender"].replace(" ", "").lower() if row["gender"] != 0 else "u"
        title = unidecode.unidecode(str(row["title"]).encode("utf-8").decode("ascii", "ignore").title()) if row["title"] != 0 else ""
        cell_phone = str(row["cell_phone"]) if row["cell_phone"] != "0" else ""
        direct_phone = str(row["direct_phone"]) if row["direct_phone"] != "0" else ""
        
        if isinstance(row["email"], unicode):
            email = row["email"].encode("utf-8").decode("ascii", "ignore")
        else:
            email = unidecode.unidecode(str(row["email"]).decode("ISO-8859-1").strip().replace(" ", "")) if row["email"] != 0 else ""

        last_name = unidecode.unidecode(str(row["last_name"])) if row["last_name"] else ""
        photo_url = unidecode.unidecode(str(row["photo_url"]).replace(" ", "")) if row["photo_url"] != 0 else ""
        linkedin_url = unidecode.unidecode(str(row["linkedin_url"]).replace(" ", "")) if row["linkedin_url"] != 0 else None
        facebook_url = unidecode.unidecode(str(row["facebook_url"]).replace(" ", "")) if row["facebook_url"] != 0 else None
        twitter_url = unidecode.unidecode(str(row["twitter_url"]).replace(" ", "")) if row["twitter_url"] != 0 else None

        if has_open_opp or is_in_program:
            update_params = (last_name, company_id, gender, email, cell_phone, facebook_url, twitter_url, linkedin_url, direct_phone, photo_url, contact_id)
            update_sql = """UPDATE xrms.contacts SET last_name=%s, company_id=%s, gender=%s, email=%s, cell_phone=%s, facebook=%s, twitter=%s, linkedin=%s, direct_phone=%s, source_photo_url=%s, last_modified_by=2769, last_modified_at=current_timestamp() WHERE contact_id=%s;"""
        else:
            update_params = (last_name, company_id, gender, title, email, cell_phone, facebook_url, twitter_url, linkedin_url, direct_phone, photo_url, contact_id)
            update_sql = """UPDATE xrms.contacts SET last_name=%s, company_id=%s, gender=%s, title=%s, email=%s, cell_phone=%s, facebook=%s, twitter=%s, linkedin=%s, direct_phone=%s, source_photo_url=%s, last_modified_by=2769, last_modified_at=current_timestamp() WHERE contact_id=%s;"""

        log.info("update_params: {up}".format(up=update_params))

        try:
            if contact_id > 0:
                update_cur = execute_query(dbmaster, update_sql, update_params, db_name="CRM")
                dbmaster.commit()
                log.info("Update successful.")

                log.info("Checking contact info...")
                contact_info_list_db = get_contact_info(contact_id)
                
                if email:
                    log.info("Checking for contact info email... {}".format(email))
                    cid = check_db_for_user(email, company_id, is_email=True, contact_id=contact_id)

                    if cid > 0:
                        log.info("Contact info found!")
                    else:
                        log.info("Inserting email...")
                        contactinfo_insert(contact_id, email, ctype="e")
                        log.info("Done.")

                if cell_phone:
                    _adjust_contact_info(contact_info_list_db, contact_id, cell_phone, "Cell")
                    
                if direct_phone:
                    _adjust_contact_info(contact_info_list_db, contact_id, direct_phone, "Direct")

            else:
                log.error("Contact ID equals zero!  Skipping.")

        except mysql.Error as e:
            log.error("Insert failed with MySQL error: {e}".format(e=e))
            log.error(traceback.format_exc())

def process_insert(insert_df, company_id):
    log.debug("-------------------------------------------------------------------------------------------------------------------------------------------------------------")
    insert_sql = """INSERT INTO xrms.contacts (company_id, last_name, first_names, gender, title, email, cell_phone, facebook, twitter, linkedin, entered_at, entered_by, direct_phone, source_photo_url)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp(), 2769, %s, %s);"""

    for idx, row in insert_df.iterrows():
        contact_found = False  # Flag to track if contact has been found for the current row
        log.info("Processing insert record: {iu}".format(iu=idx + 1))
        try:
            first_name = unidecode.unidecode(
                row["first_name"].encode("utf-8").decode("ascii", "ignore").strip().title()) if row["first_name"] != 0 else ""
            
        except AttributeError:
            first_name = str(row["first_name"]) if row["first_name"] != "0" else ""
            log.warning(f"AttributeError in {first_name}")

        try:
            last_name = unidecode.unidecode(
                row["last_name"].encode("utf-8").decode("ascii", "ignore").strip().title()) if row["last_name"] != 0 else ""
            
        except AttributeError:
            last_name = str(row["last_name"]) if row["last_name"] != "0" else ""
            log.warning(f"AttributeError in {last_name}")

        gender = row["gender"].replace(" ", "").lower() if row["gender"] != 0 else "u"
        
        try:
            title = unidecode.unidecode(str(row["title"]).encode("utf-8").decode("ascii", "ignore").title()) if row["title"] != 0 else ""
        except AttributeError:
            title = str(row["title"]) if row["title"] != "0" else ""
            log.warning(f"AttributeError in {title}")

        cell_phone = str(row["cell_phone"]) if row["cell_phone"] != "0" else ""
        direct_phone = str(row["direct_phone"]) if row["direct_phone"] != "0" else ""
        email = str(row["email"]).replace(" ", "").encode("utf-8").decode("ascii", "ignore") if row["email"] != 0 else ""
        photo_url = str(row["photo_url"]).replace(" ", "") if row["photo_url"] != 0 else ""
        linkedin_url = str(row["linkedin_url"]).replace(" ", "") if row["linkedin_url"] != 0 else None
        facebook_url = str(row["facebook_url"]).replace(" ", "") if row["facebook_url"] != 0 else None
        twitter_url = str(row["twitter_url"]).replace(" ", "") if row["twitter_url"] != 0 else None

        insert_params = (
            company_id, last_name, first_name, gender, title, email, cell_phone, facebook_url, twitter_url,
            linkedin_url, direct_phone, photo_url)

        ## Check for existing rec on email and phone numbers
        log.info("Checking for existing contact based on contact info")
        contact_found = False
        update_existing = True
        can_update = False  # Initialize can_update

        if update_existing:
            if email != "":
                log.info("Checking for contact by email... {}".format(email))
                contact_info = check_db_for_user_insert(email, company_id, is_email=True)
                if contact_info is not None:
                    for _, row_info in contact_info.iterrows():
                        contact_id = row_info['userid']
                        if (str(row_info['company_id']).strip() == str(company_id).strip() and
                                str(row_info['last_name']).strip() == last_name and
                                str(row_info['first_name']).strip() == first_name):
                            log.info("Contact found! Contact ID: {cid}".format(cid=contact_id))
                            contact_found = True
                            break
                        elif (str(row_info['company_id']).strip() != str(company_id).strip() and
                              str(row_info['last_name']).strip() == last_name and
                              str(row_info['first_name']).strip() == first_name):
                            log.info("Contact found in different office: {company_id}! Check to see if they are in program, have open opportunity on them, or are the DM of another office. Contact ID: {cid}".format(
                                cid=contact_id, company_id=row_info['company_id']))
                            contact_found = True
                            break

            if not contact_found and cell_phone != "":
                log.info("Checking for contact by cell phone... {}".format(cell_phone))
                contact_info = check_db_for_user_insert(cell_phone, company_id)
                if contact_info is not None:
                    for _, row_info in contact_info.iterrows():
                        contact_id = row_info['userid']
                        if (str(row_info['company_id']).strip() == str(company_id).strip() and
                                str(row_info['last_name']).strip() == last_name and
                                str(row_info['first_name']).strip() == first_name):
                            log.info("Contact found! Contact ID: {cid}".format(cid=contact_id))
                            contact_found = True
                            break
                        elif (str(row_info['company_id']).strip() != str(company_id).strip() and
                              str(row_info['last_name']).strip() == last_name and
                              str(row_info['first_name']).strip() == first_name):
                            log.info("Contact found in different office: {company_id}! Check to see if they are in program, have open opportunity on them, or are the DM of another office. Contact ID: {cid}".format(
                                cid=contact_id, company_id=row_info['company_id']))
                            contact_found = True
                            break

            if not contact_found and direct_phone != "":
                log.info("Checking for contact by direct phone... {}".format(direct_phone))
                contact_info = check_db_for_user_insert(direct_phone, company_id)
                if contact_info is not None:
                    for _, row_info in contact_info.iterrows():
                        contact_id = row_info['userid']
                        if (str(row_info['company_id']).strip() == str(company_id).strip() and
                                str(row_info['last_name']).strip() == last_name and
                                str(row_info['first_name']).strip() == first_name):
                            log.info("Contact found! Contact ID: {cid}".format(cid=contact_id))
                            contact_found = True
                            break
                        elif (str(row_info['company_id']).strip() != str(company_id).strip() and
                              str(row_info['last_name']).strip() == last_name and
                              str(row_info['first_name']).strip() == first_name):
                            log.info("Contact found in different office: {company_id}! Check to see if they are in program, have open opportunity on them, or are the DM of another office. Contact ID: {cid}".format(
                                cid=contact_id, company_id=row_info['company_id']))
                            contact_found = True
                            break

        if not contact_found:
            log.info("No existing contact found. Inserting...")
            log.info("Insert_params: {ip}".format(ip=insert_params))
            log.info("Insert query: {isq}".format(isq=insert_sql))

            try:
                insert_cur = execute_query(dbmaster, insert_sql, insert_params, db_name="CRM")
                dbmaster.commit()
                # Get inserted contact ID
                contact_id = insert_cur.lastrowid
                log.info("Insert successful. New contact ID: {nc}".format(nc=contact_id))
                log.info("Inserting contact info for new contact...")

                if cell_phone != "":
                    log.info("Inserting cell phone...")
                    contactinfo_insert(contact_id, cell_phone, ctype="c", source_column="cell_phone")
                    log.info("Done.")

                if direct_phone != "":
                    log.info("Inserting direct phone...")
                    contactinfo_insert(contact_id, direct_phone, ctype="d")
                    log.info("Done.")

                if email != "":
                    log.info("Inserting email...")
                    contactinfo_insert(contact_id, email, ctype="e")
                    log.info("Done.")

                log.info('----------------------------------------------------------------------------------------------')

            except mysql.Error as e:
                log.error("Insert failed with MySQL error: {e}".format(e=e))
                log.error(traceback.format_exc())

        else:
            log.info("A contact was found, checking if deleted...".format(cid=contact_id))
            is_deleted = check_if_deleted(dbmaster, contact_id)

            if not is_deleted:
                can_update = process_not_found_insert(contact_id)

                if can_update:
                    log.info("Contact is active, updating...")

                    update_params = (
                    company_id, last_name, first_name, gender, title, email, cell_phone, facebook_url, twitter_url,
                    linkedin_url, direct_phone, photo_url, contact_id)
                    log.info("update_params: {up}".format(up=update_params))
                    update_sql = """UPDATE xrms.contacts
                    SET company_id=%s, last_name=%s, first_names=%s, gender=%s, title=%s, email=%s, cell_phone=%s, facebook=%s, twitter=%s, linkedin=%s, direct_phone=%s, source_photo_url=%s, last_modified_by=2769, last_modified_at=current_timestamp()
                    WHERE contact_id=%s;"""

                    try:
                        upd_cnt = execute_query(dbmaster, update_sql, update_params, db_name="CRM")
                        log.info("Updating contact ID: {cid}".format(cid=int(contact_id)))
                        log.info("Done. Rows affected: {ra}".format(ra=upd_cnt))
                        dbmaster.commit()
                        contact_info_list_db = get_contact_info(contact_id)

                        if email != "":
                            log.info("Checking for contact info email... {}".format(email))
                            cid = check_db_for_user(email, company_id, is_email=True, contact_id=contact_id)
                            if cid > 0:
                                log.info("Contact info found!")
                            else:
                                log.info("Inserting email...")
                                contactinfo_insert(contact_id, email, ctype="e")
                                log.info("Done.")

                        if cell_phone:
                            _adjust_contact_info(contact_info_list_db, contact_id, cell_phone, "Cell")
                            
                        if direct_phone:
                            _adjust_contact_info(contact_info_list_db, contact_id, direct_phone, "Direct")

                    except mysql.Error as e:
                        log.error("Update failed with MySQL error: {e}".format(e=e))
                        log.error(traceback.format_exc())
                    finally:
                        log.info('----------------------------------------------------------------------------------------------')
                else:
                    log.info('Open workflows found....skipping update.')

            else:
                log.info('Contact was deleted. Creating new contact..')
                log.info("Insert_params: {ip}".format(ip=insert_params))
                log.info("Insert query: {isq}".format(isq=insert_sql))

                try:
                    insert_cur = execute_query(dbmaster, insert_sql, insert_params, db_name="CRM")
                    dbmaster.commit()
                    # Get inserted contact ID
                    contact_id = insert_cur.lastrowid
                    log.info("Insert successful. New contact ID: {nc}".format(nc=contact_id))
                    log.info("Inserting contact info for new contact...")

                    if cell_phone != "":
                        log.info("Inserting cell phone...")
                        contactinfo_insert(contact_id, cell_phone, ctype="c", source_column="cell_phone")
                        log.info("Done.")

                    if direct_phone != "":
                        log.info("Inserting direct phone...")
                        contactinfo_insert(contact_id, direct_phone, ctype="d")
                        log.info("Done.")

                    if email != "":
                        log.info("Inserting email...")
                        contactinfo_insert(contact_id, email, ctype="e")
                        log.info("Done.")

                    log.info('----------------------------------------------------------------------------------------------')

                except mysql.Error as e:
                    log.error("Insert failed with MySQL error: {e}".format(e=e))
                    log.error(traceback.format_exc())
                    log.info('----------------------------------------------------------------------------------------------')

def process_not_found_insert(contact_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    log.info("Checking status of contact ID: {cid}".format(cid=contact_id))

    can_update = False

    if contact_id > 0:
        log.info("Checking customer status...")
        if not customer_check(contact_id):
            log.info("The contact is not a customer.")
            log.info("Checking for any relationship sets...")
            if not check_relationship_set(contact_id):
                log.info("The contact is not part of a relationship")
                if not open_workflows_check(contact_id):
                    log.info("No open workflows found.")
                    can_update = True
                else:
                    log.info("Open workflow(s) found!  Skipping this contact.")
            else:
                log.info("Open relationship found!  Skipping this contact.")
        else:
            log.info("This contact is a customer!  Skipping.")

    else:
        log.error("Invalid contact ID: {cid}".format(cid=contact_id))

    return can_update


def lookup_activity_id(workflow_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    log.info("Getting activity_id for workflow ID: {wid}".format(wid=workflow_id))

    lookup_activity_id_sql = """SELECT
	a.activity_id
FROM
	xrms.activities a 
WHERE
	1=1
	AND a.on_what_table = 'cases' 
	AND on_what_id = {workflow_id} 
	AND activity_template_id IN (2476,606,3110)
	AND activity_record_status = 'a';"""

    lookup_activity_id_cur = execute_query(dbmaster, lookup_activity_id_sql.format(workflow_id=workflow_id), db_name="CRM")

    activity_id = lookup_activity_id_cur.fetchone()

    if activity_id is not None:
        log.info("Activity ID found: {at}".format(at=activity_id[0]))
        return activity_id[0]
    else:
        log.error("No activity ID could be found!")
        return 0


def update_notes_table(workflow_id, note_text):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    activity_id = lookup_activity_id(workflow_id)

    notes_insert_sql = """INSERT INTO xrms.notes (note_description, on_what_table, on_what_id, entered_at, entered_by, dismissed_at, dismissed_by, note_record_status, parent_note_id, category_id, deleted_by, activity_id, case_id, opportunity_id, system_note, flags, lifespan_start, lifespan_end)
VALUES(%s, 'cases', %s, current_timestamp(), 2769, '0000-00-00 00:00:00', 0, 'a', 0, 5, 0, %s, 0, 0, 0, 0, '0000-00-00', '0000-00-00');"""

    notes_insert_params = (note_text, workflow_id, activity_id)

    try:
        notes_insert_sql = execute_query(dbmaster, notes_insert_sql, notes_insert_params, db_name="CRM")
        dbmaster.commit()
        # Get inserted ID
        note_insert_id = notes_insert_sql.lastrowid
        log.info("Insert successful.  Inserted ID: {nc}".format(nc=note_insert_id))
    except mysql.Error as e:
        log.error("Insert failed with MySQL error: {e}".format(e=e))
        log.error(traceback.format_exc())


def lookup_workflow_id(office_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    # Parameterized SQL Query
    lookup_workflow_sql = """SELECT 
                                ora.workflow_id
                             FROM 
                                bi_warehouse_prd.office_roster_automation_f ora
                             WHERE 
                                ora.office_id = %s
                             ORDER BY 
                                ora.workflow_id DESC;"""

    try:
        log.info(f"Looking up workflow ID for Office: {office_id}")

        lookup_workflow_cur = execute_query(bi, lookup_workflow_sql, (office_id,), db_name="BI")

        response = lookup_workflow_cur.fetchone()
        log.info(response)

        if response is not None:
            workflow_id = response[0]

    except mysql.Error as e:
        log.error(f"Query failed with MySQL error: {e}")
        log.error(traceback.format_exc())

    return workflow_id if response else None


def update_roster_status(workflow_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))
    ## Set to manager review

    params = {
        "api_key": SECRETS.get("update_case_status_api_key"),
        "workflows": [{
            "workflow_id": workflow_id,
            "workflow_type": "case",
            "status_id": 433
        }
        ]
    }

    r = requests.post(SECRETS.get("update_case_status_api_url"), json=params, verify=False)
    log.info("Response: {rr}".format(rr=r))

    print(r.status_code)

    if r.status_code != 200:
        message = "Status code is: {sc}, for update roster status. Please investigate".format(sc=r.status_code)
        send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=message)

def final_review_roster_status(workflow_id):
    log.info("Entering final_review_roster_status")

    params = {
        "api_key": SECRETS.get("update_case_status_api_key"),
        "workflows": [{
            "workflow_id": workflow_id,
            "workflow_type": "case",
            "status_id": 1443
        }
        ]
    }

    r = requests.post(SECRETS.get("update_case_status_api_url"), json=params, verify=False)

    log.info("Response: {rr}".format(rr=r))

    if r.status_code != 200:
        message = "Status code is: {sc}, for final review roster status. Please investigate".format(sc=r.status_code)
        send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=message)

def update_stats_table(params):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    update_sql = """UPDATE bi_warehouse_prd.office_roster_automation_f SET received_records=%s, received_records_not_found=%s, received_records_update=%s, received_records_insert=%s, received_from_email=%s, received_filename=%s, received_dtt=%s WHERE office_id=%s;"""

    try:
        update_cur = execute_query(bi, update_sql, params, db_name="BI")
        bi.commit()
        log.info("Update successful.")
    except mysql.Error as e:
        log.error("Update failed with MySQL error: {e}".format(e=e))
        log.error(traceback.format_exc())
    # Perform a SELECT statement to check the table after the update
    select_sql = """
            SELECT office_id, received_records, received_records_not_found, received_records_update, received_records_insert, received_from_email, received_filename, received_dtt FROM bi_warehouse_prd.office_roster_automation_f 
            WHERE office_id = %s 
            AND office_roster_automation_f_id = (
                SELECT MAX(office_roster_automation_f_id) 
                FROM bi_warehouse_prd.office_roster_automation_f 
                WHERE office_id = %s);"""

    try:
        select_cur = execute_query(bi, select_sql, [params[7], params[7]], db_name="BI")
        rows = select_cur.fetchall()
        log.info("Table contents after update:")
        missing_columns = []

        for row in rows:
            if None in row[1:7]:  # Check if any of the specific columns is None
                missing_columns.append(row[0])  # Append the office_id to the list
        if missing_columns:
            log.error(f"Error: Some rows are missing specific columns. Office IDs: {missing_columns}")
            send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                           text=f"Some rows are missing specific columns for office IDs: {', '.join(map(str, missing_columns))}")
        else:
            log.info("All rows have the required columns.")

    except Exception as e:
        log.error(f"An error occurred: {str(e)}")
        log.error(traceback.format_exc())


def check_for_open_opps(contact_id):
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    opp_sql = """SELECT
	of.opportunity_id
FROM
	bi_warehouse_prd.opportunity_f of
WHERE
	1=1
	AND of.contact_id = {cid} 
	AND open_status = 'Open';"""

    opp_cur = execute_query(bi, opp_sql.format(cid=contact_id), db_name="BI")

    resp = opp_cur.fetchone()

    if resp is not None:
        return resp[0]
    else:
        return 0

def check_cases_table(workflow_id):
    lookup_case_status = """
    SELECT
        c.case_id AS workflow_id,
        c.case_status_id
    FROM
        xrms.cases c
    WHERE
	    c.case_id = %s"""

    try:
        lookup_cur = execute_query(dbmaster, lookup_case_status, (workflow_id,), db_name="CRM")
        result = lookup_cur.fetchone()

        if result:
            on_what_id, on_what_status = result
            log.info(f"Workflow ID: {on_what_id}, Status ID : {on_what_status}")
            return on_what_status
        else:
            log.info(f"No records found for Workflow ID: {workflow_id}")
            return None

    except mysql.Error as e:
        log.error(f"Query failed with MySQL error: {e}")
        return None


def create_directories():
    """ Create the necessary directories if they don't exist """
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(ERROR_DIR, exist_ok=True)

def get_cleaned_roster_from_email():
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    log.info("Getting inbox items matching filter...")
    lookback_dt = (
        datetime.datetime.now(tz=account.default_timezone)
        - datetime.timedelta(days=EMAIL_LOOKBACK_DAYS)
    )
    items = account.inbox.filter(
        subject__contains=EMAIL_SUBJECT,
        has_attachments=True,
        datetime_received__gte=lookback_dt
    ).order_by("datetime_received")
        
    if items.count() > 0:
        log.info("Items found: ".format(items.count()))

        for item in items:
            time.sleep(2)
            from_email = item.sender.email_address
            log.info("Message sender: {fe}".format(fe=from_email))
            received_ts = str(item.datetime_received)[:-6]
            log.info("Message received timestamp: {rt}".format(rt=received_ts))

            for attachment in item.attachments:
                if isinstance(attachment, FileAttachment):
                    log.info("Attachment found: {an} - Checking file attachement...".format(an=attachment.name))
                    att_name = attachment.name.split(".")
                    att_file_name = att_name[0]
                    att_file_ext = att_name[1]
                    
                    if att_file_ext == "xls" or att_file_ext == "xlsx":
                        local_path = os.path.join(SCRIPT_PATH, attachment.name)
                        log.info(f"Attachment local path {local_path}")
                        backup_path = os.path.join(BACKUP_DIR, attachment.name)
                        time.sleep(5)  ## Adding a 5 second delay for the file to finish being saved to see if this helps with file not found errors
                        
                        with open(local_path, 'wb') as f:
                            f.write(attachment.content)
                        log.info('Saved attachment to {lp}'.format(lp=local_path))
                        
                        #TODO: Saving the file in a "process_queue" directory is all this function should do.
                        log.info("Processing returned file: {sn}".format(sn=attachment.name))
                        
                        ## Get office_id from file name
                        log.info("Getting office ID from file name...")
                        office_id_from_filename = filter_non_numeric(att_file_name.split("_")[0])
                        office_id = office_id_from_filename
                    
                        if int(office_id_from_filename) > 0:
                            log.info("Office ID: {oid}".format(oid=office_id))
                        else:
                            e = "Office ID ({oid}) from file name ({fn}) is not found or invalid!".format(
                                oid=office_id_from_filename, fn=att_file_name)
                            log.critical(e)
                            log.critical("Kicking the file to the error directory...")
                            log.critical("Moving returned file {sn} to error folder...".format(sn=attachment.name))
                            
                            if os.path.exists(ERROR_DIR):
                                if os.path.exists(local_path):
                                    error_path = os.path.join(ERROR_DIR, attachment.name)
                                    os.rename(local_path, error_path)
                                    log.critical("Report file moved to error folder.")
                            continue
                            
                        
                        ## Get data
                        if already_processed(bi, office_id, attachment.name):
                            log.info(
                                "Attachment {name} for office ID {oid} already processed. Skipping.".format(
                                    name=attachment.name,
                                    oid=office_id
                                )
                            )
                            continue
                        log.info("Getting returned data...")
                        # returned_data_df = pd.read_excel(local_path, sheet_name="Office {oid}".format(oid=office_id), skiprows=13)
                        try:
                            returned_data_df = pd.read_excel(local_path, engine='openpyxl', sheet_name="Office {oid}".format(oid=office_id), skiprows=13)
                        except Exception as e:
                            send_teams_message(
                                summary=SEND_TEAMS_MESSAGE_SUMMARY,
                                activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE,
                                activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                text=str(e),
                                webhook_url=WEBHOOK_KEY_TO_USE,
                            )
                            mark_read_with_retry(item)
                            continue
                        returned_data_df_len = len(returned_data_df)
                        log.info("Data loaded.  Records found: {rf}".format(rf=returned_data_df_len))
                        log.info("Checking column names...")
                        returned_data_df.columns = list(map(str.lower, returned_data_df.columns))
                        returned_data_df.columns = returned_data_df.columns.str.replace(" ", "")
                        column_list = ["notfound", "contact_id", "office_id", "first_name", "last_name", "gender",
                                       "title", "cell_phone", "direct_phone", "email", "photo_url", "linkedin_url",
                                       "facebook_url", "twitter_url", "zillow_url"]
                        
                        for column_name in column_list:
                            col_found = False
                            if column_name in returned_data_df.columns:
                                col_found = True

                            log.info("Looking for column \"{cn}\" in dataframe... {cf}".format(cn=column_name, cf="Found." if col_found else "Not found!"))

                            if not col_found:
                                log.critical("Column \"{cn}\" was not found in the dataframe.  Exiting.".format(cn=column_name))
                                ## Kick out the file
                                log.critical("Kicking the file to the error directory...")
                                log.critical("Moving returned file {sn} to error folder...".format(sn=attachment.name))
                                error_path = os.path.join(ERROR_DIR, attachment.name)
                                os.rename(local_path, error_path)
                                log.critical("Report file moved to error folder.")
                                
                                #TODO: THERE SHOULD BE A FUNCTION IN ETL_UTILITIES TO DO THIS.
                                log.critical("Marking email message as read...")
                                mark_read_with_retry(item)
                                log.critical("Message marked as read.")

                        returned_data_df["notfound"] = returned_data_df["notfound"].fillna("")
                        returned_data_df["contact_id"] = returned_data_df["contact_id"].fillna(0).apply(filter_non_numeric)
                        returned_data_df["contact_id"] = returned_data_df["contact_id"].astype("int")

                        ## Split returned dataframe into 'not found', 'update', and 'insert'
                        ## For 'not found' - before soft delete, make sure that they are 1) not a customer; and 2) do not have any open workflows
                        ## For 'insert' verify that they do not have an existing customer record in contact_info table with email and phone #'s

                        log.info("Creating 'not found' dataframe...")
                        not_found_df = returned_data_df[returned_data_df["notfound"].str.lower() == "x"].reset_index(drop=True).fillna(0)
                        not_found_df_len = len(not_found_df)
                        log.info("Dataframe created.  Rows: {nfr}".format(nfr=not_found_df_len))

                        log.info("Creating 'update' dataframe...")
                        update_df = returned_data_df[(returned_data_df["notfound"].str.lower() != "x") & (
                                    returned_data_df["contact_id"] > 0)].reset_index(drop=True).fillna(0)
                        update_df["cell_phone"] = update_df["cell_phone"].apply(clean_phone_number)
                        update_df["direct_phone"] = update_df["direct_phone"].apply(clean_phone_number)
                        update_df_len = len(update_df)
                        log.info("Dataframe created.  Rows: {ufr}".format(ufr=update_df_len))

                        log.info("Creating 'insert' dataframe...")
                        insert_df = returned_data_df[returned_data_df["contact_id"] == 0].reset_index(drop=True).fillna(0)
                        insert_df["cell_phone"] = insert_df["cell_phone"].apply(clean_phone_number)
                        insert_df["direct_phone"] = insert_df["direct_phone"].apply(clean_phone_number)
                        insert_df_len = len(insert_df)
                        log.info("Dataframe created.  Rows: {ifr}".format(ifr=insert_df_len))

                        ## Get the returned employee count
                        emp_cnt = update_df_len + insert_df_len

                        # TODO: THIS FUNCTION SHOULD RETURN THE DATAFRAMES, AND CALLING THE PROCESS_XXXX FUNCTIONS SHOULD BE DONE IN MAIN 
                        log.info("Checking 'not found' dataframe...")
                        if not_found_df_len > 0:
                            log.info("Records found.  Processing 'not found' dataframe.")
                            process_not_found(not_found_df)
                        else:
                            log.info("No records found to process on 'not found' dataframe.")

                        log.info("Checking 'update' dataframe...")
                        if update_df_len > 0:
                            log.info("Records found.  Processing 'update' dataframe.")
                            process_update(update_df, office_id)
                        else:
                            log.info("No records found to process on 'update' dataframe.")

                        log.info("Checking 'insert' dataframe...")
                        if insert_df_len > 0:
                            log.info("Records found.  Processing 'insert' dataframe.")
                            process_insert(insert_df, office_id)
                        else:
                            log.info("No records found to process on 'insert' dataframe.")

                        ## Get workflow_id based on office_id from stats table
                        workflow_id = lookup_workflow_id(office_id)
                        if workflow_id > 0:
                            log.info("Workflow ID found!  Workflow ID: {wid}".format(wid=workflow_id))
                        else:
                            log.error("No workflow ID found.")

                        ## Check for notes and set status
                        log.info("Getting header data and checking for notes...")
                        header_data_df = pd.read_excel(local_path, nrows=12, usecols="B:C", header=None)
                        header_data_df = header_data_df.dropna().rename(columns={1: "keys", 2: "values"})
                        if "notes" in header_data_df["keys"].str.lower().tolist():
                            log.info("Note found!  Processing...")
                            note_text = \
                            header_data_df["values"].loc[header_data_df["keys"].str.lower() == "notes"].values[0]
                            if isinstance(note_text, str):
                                n_text = note_text.encode("utf-8").decode("ascii", "ignore")
                            else:
                                n_text = unidecode.unidecode(note_text.decode("ISO-8859-1"))
                            log.info("The note reads: {nn}".format(nn=n_text))
                            log.info("Inserting note...")
                            update_notes_table(workflow_id, n_text)
                            log.info("Updating roster status...")
                            update_roster_status(workflow_id)
                            log.info("Done.")
                        else:
                            log.info("No note found.  Setting result...")
                            final_review_roster_status(workflow_id)

                        ## Processing finished
                        log.info("Returned file processing finished.")
                        log.info("Updating stats table...")
                        params = [returned_data_df_len, not_found_df_len, update_df_len, insert_df_len, from_email,
                                  attachment.name, received_ts, office_id]
                        update_stats_table(params)
                        update_companies_table(office_id, emp_cnt)
                        log.info("Moving returned file {sn} to backup folder...".format(sn=attachment.name))
                        if os.path.exists(local_path):
                            os.rename(local_path, backup_path)
                            log.info("Report file moved to backup folder.")

                        status = check_cases_table(workflow_id)
                        if status == 433:
                            log.info("Status has changed from 1441 to 433")
                        elif status == 1443:
                            log.info("Status has changed from 1441 to 1443")
                        else:
                            error_message = f"Please Investigate: workflow_id {workflow_id} for office_id {office_id}. Status did not update from {status}"
                            log.info(error_message)
                            send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                                               activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=error_message)

                        log.info("Marking email message as read...")
                        mark_read_with_retry(item)
                        log.info("Message marked as read.")

                    else:
                        log.error("Attachment type invalid!  Must be .xls or .xlsx!")
    else:
        log.info("No COR emails found to process.")


def main():
    
    start_ts = time.time()  
    log.debug("Entering {}()".format(sys._getframe().f_code.co_name))

    try:
        create_directories()
        get_cleaned_roster_from_email()
    except mysql.Error as err:
        log.error(f"An error occurred with MySQL: {err}")
        log.error("Error code:", err.args[0])
        log.error("Error message:", err.args[1])
        log.error("Full error:", str(err))
        log.error(traceback.format_exc())
        send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=err)
        sys.exit(1)

    except Exception as e:
        log.critical("Critical error has occurred, sounding alarm!  Error info: {e}".format(e=e))
        log.error(traceback.format_exc())
        send_teams_message(summary=SEND_TEAMS_MESSAGE_SUMMARY, activityTitle=SEND_TEAMS_MESSAGE_ACTIVITY_TITLE, 
                           activitySubtitle=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), text=e)
        sys.exit(1)

    finally:
        log.info("Closing database connection...")
        dbmaster.close()
        log.info("Database connection closed.")
        end_ts = time.time()
        final_time = end_ts - start_ts
        log.info("Processing finished.  Time: {mt} minutes ({et} seconds)".format(mt=round(final_time / 60, 3),
                                                                                  et=round(final_time, 3)))
        log.info("-" * 120)


if __name__ == '__main__':
    main()
