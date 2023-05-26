"""Module to provide data management functions for accessing and retrieving
   external source data, formatting and database inserting for storagetooling
   database used with Scout application
"""
import struct
import sys
import time
import os
import asyncio

from datetime import datetime
from csv import DictReader
from operator import itemgetter

import pyodbc

from storagetooling import general
from storagetooling import sannav as dbquery
from storagetooling import dns_webstack as stackdns

# Storage Tooling Database Tables
Storage_Core_Tables = [
    'activezones',
    'customers',
    'webs'
]

Storage_Id_Tables = [
    'entitytype',
    'fabric',
    'health',
    'run',
    'stacks',
    'status',
    'switch',
    'webpools',
    'zone'
]

# Default SQL connection value string dictionary
Default_Sql_Connection_Values = {
    "Driver": "{ODBC Driver 18 for SQL Server}",
    "Server": "pupdbprod102a.prod.athenahealth.com",
    "Database": "storagetooling",
    "Trusted_Connection": "yes",
    "TrustServerCertificate": "yes"
}


# Assorted global lists defined
sannav_object_insert_dict_list = []
activezone_object_insert_dict_list = []
debug_log = []
activezone_object_id_table_dict_list = []

# Global Class object intialization
storage_data_object = general.StorageData()
storagetooling_tables_cache = general.StoragetoolingTableCache()

def __clear_table(
        table,
        conn
    ):
    """Internal Function for deleting a table of all records.

    Args:
        table (String, required): The name of the table to execute against

        conn (Pyodbc connection object, required): The established SQL connection object
        that has connected to the desired database

    Object:
        Result (Boolean): The result of the attempted delete; True/False
    """

    cursor = conn.cursor()

    # Craft SQL DELETE statement and attempt to clear table
    try:
        delete_stement = f'TRUNCATE TABLE [dbo].[{table}]'

        cursor.execute(delete_stement)
        conn.commit()

        result = True
    except ResourceWarning:
        result = False
        print(f'ERROR: clearing table {table}')

    return result

def __connect_ms_sql(
        username = '',
        password = '',
        db_server = Default_Sql_Connection_Values["Server"],
        database = Default_Sql_Connection_Values["Database"],
        driver = Default_Sql_Connection_Values["Driver"],
        trust_certificates = Default_Sql_Connection_Values["TrustServerCertificate"]
    ):
    """Internal Function for creating a re-usable secure connectiion to a SQL database.

    Args:
        username (String, optional): The user name to use for connecting to SQL.
        The default is blank and will instead use the current username of the initiating user.

        password (String, optional): The password name to use for connecting to SQL.
        The default is blank and will instead use the current credentials of the initiating user.

        db_server (String, optional): The DNS name of the sql database server to connect to.
        The default value is the global dictionary Default_Sql_Connection_Values["Server"] value.

        database (String, optional): The name of the database to connect to.
        The default value is the global dictionary Default_Sql_Connection_Values["Database"] value.

        driver (String, optional): The locally installed ODBC DataSource Drive name to be used on
        the mahcine wher the code is running from.
        The default value is the global dictionary Default_Sql_Connection_Values["Driver"] value.

        trust_certificates (String, optional): The value to trust unverified certificates (yes/no).
        The default is the dictionary Default_Sql_Connection_Values["TrustServerCertificate"] value.

    Object:
        SQL connection objet (object): An active connection object that can be used to initiate
        cursor connections and send SQL commands for execution
    """

    # Default value to require a username/password and not use
    # the current logged in user credentials
    trusted_connection = "no"

    # Determine if credentials were passed or to use current user credentials;
    # then determine connection string parameters
    if username == '' or password == '':
        trusted_connection = Default_Sql_Connection_Values["Trusted_Connection"]
        print("Using trusted connection and current user credentials")

    if trusted_connection == "yes":
        connect_str = (f'Driver={driver};Server={db_server};Database={database};'
        f'Trusted_Connection={trusted_connection};TrustServerCertificate={trust_certificates}')
    else:
        connect_str = (f'Driver={driver};Server={db_server};Database={database};'
        f'UID={username};PWD={password};TrustServerCertificate={trust_certificates}')

    # Attempt to connect to SQL using the connection parameters from above
    try:
        print(f'Connection string used for connecting: {connect_str}')
        sql_connection = pyodbc.connect(connect_str)
    except ResourceWarning:
        print('ERROR: MS SQL connection negotiation issue')

    # Add required internal functions to handle data types not handled by default ()
    sql_connection.add_output_converter(-151, __handle_hierarchy_id)
    sql_connection.add_output_converter(-155, __handle_datetimeoffset)

    return sql_connection

async def __customer_lookup_async(**kwargs):
    """Internal Function for creating a list of concurrent stack runs and running the jobs.

    Args:
        stack_object (List, required): The list object with all the DNS records to query

    Object:
        Stack lookup result object(List): List of each stack DNS lookup result object
    """

    returned_exceptions = kwargs.pop('return_exceptions', True)

    coros = [__format_customer_insert_data(customer_object)
            for customer_object in storage_data_object.customers]

    return await asyncio.gather(*coros, return_exceptions = returned_exceptions)

async def __format_customer_insert_data(customer):
    """Internal Function for formatting athenaOne customer data
    to be inserted into the customers table in storagetooling database.

    Args:
        customer (Dictionary, required): The customer dictionary containing MTEST customer data

    Object:
        Formatted customer data (Dictionary): Customer dictionary formatted for insertion
        into the customers table in storagetooling DB
    """

    # Convert customer live value from MTEST results to binary value for database
    if customer['L'] == 'Y':
        status = '1'
    else:
        status = '0'

    # Look for apostrphes and format using double apostrophes needed by SQL
    if "'" in customer['NAME']:
        customer_name = (customer['NAME']).replace("'","''")
    else:
        customer_name = customer['NAME']

    #Create customer data dictionary to be returned
    customer_data_formatted = {
        'contextid': customer['ID'],
        'customer': customer_name,
        'status': status,
        'stackid': customer['STACKNUMBER']}

    return customer_data_formatted

async def __format_sannav_insert_data(
            sannav_object,
            runid
        ):
    """Internal Function for formatting SanNav data to be inserted into the sannaventities table in storagetooling database.

    Args:
        sannav_object (Dictionary, required): The sannav dictionary data from the SanNav API

        runid (String, required): Current session's runId from database

    Object:
        Formatted sannav data (Dictionary): SanNav dictionary formatted for insertion
        into the sannaventities table in storagetooling DB
    """

    # Set the default value for sannav parameters that need to be standardized
    device = 'none'
    formated_records = None

    # List object used to contain activezone entries that will need to be added
    # to the ActiveZones table in storagetooling DB
    activezones_results = []
    formated_records = {}

    # List to use for iterating through simple ID tables that are used by SanNavEntities table
    sannav_common_id_tables = [
        'entitytype',
        'fabric',
        'health',
        'status',
        'switch',
        'zone'
    ]

    # Determine type of SanNav record type (device is something that sends data to a storage device)
    if sannav_object['connectedDeviceType']:
        if sannav_object['connectedDeviceType'] == 'Initiator':
            sannav_object['entitytype'] = 'device'
        elif sannav_object['connectedDeviceType'] == 'SWITCH':
            sannav_object['entitytype'] = 'chassis'
        elif 'Target' in sannav_object['connectedDeviceType']:
            sannav_object['entitytype'] = 'storage'

    # Determine deviceName, if one exists. Many records will have ports without connections
    # and will have device = none
    if ('remoteDevice' in sannav_object.keys()) and ('remotePort' in sannav_object.keys()):
        if (sannav_object['remoteDevice'] != '') or (sannav_object['remoteDevice'] != 'localhost'):
            device = sannav_object["remoteDevice"]
        else:
            device = sannav_object['remotePort']
    elif ('remoteDevice' in sannav_object.keys()) and (sannav_object['remoteDevice'] != ''):
        device = sannav_object["remoteDevice"]
    elif ('remotePort' in sannav_object.keys()) and (sannav_object['remotePort'] != ''):
        if sannav_object['remotePort'] != '':
            device = sannav_object['remotePort']

    if len(device) > 50:
        trunk_count = -abs((len(device)) - 50)
        device = device[:trunk_count]


    # Ensure remoteNodeWwn has a value if empty
    if sannav_object['remoteNodeWwn'] == '':
        sannav_object['remoteNodeWwn'] = 'none'

    # Craft base dictionary file conmtaing the columns stored in storagetooling database.
    # Still requires ID lookups
    sannaventity_data_formatted = {
        'runid': runid,
        'id': str(sannav_object['id']),
        'wwn': sannav_object['wwn'],
        'portnumber': str(sannav_object['portNumber']),
        'slotnumber': str(sannav_object['slotNumber']),
        'remotewwn':sannav_object['remoteNodeWwn'],
        'ipaddress': sannav_object['ipAddress'],
        'devicename': device}

    # Loop through the common sannav ID tables to update sannav data with ID values for parameter
    # names to be used for database INSERT
    for sannav_id_table in sannav_common_id_tables:
        source_label = None
        search_id_table = None

        if sannav_id_table in ('switch', 'fabric'):
            source_label = f'{sannav_id_table}Name'
        elif sannav_id_table == 'zone':
            source_label = f'{sannav_id_table}'
        else:
            source_label = sannav_id_table

        search_id_table = [
            item for item in getattr(storagetooling_tables_cache, sannav_id_table)
            if item['name'] == sannav_object[str(source_label)]
        ]

        column_id = f'{source_label}id'
        search_id = search_id_table[0]['ID']
        value_id = f'{search_id}'
        sannaventity_data_formatted[column_id] = value_id

    # Create activezones entries for run
    for zone_name in sannav_object['activeZones']:
        try:
            zone_id_table = [
                item for item in storagetooling_tables_cache.zone
                if item['name'] == zone_name]

            zone_id = zone_id_table[0]['ID']

            activezones_formatted = {
                'runid': runid ,
                'sannaventityid': sannaventity_data_formatted['id'],
                'zoneid': str(zone_id)
            }

            activezones_results.append(activezones_formatted)
        except ResourceWarning:
            print('ERROR: ZoneId not found or append to list error')

    # Format result dictionary and return sannaventities and activezones data
    # to be inserted into storagetooling db
    formated_records = {
        "sannav": sannaventity_data_formatted,
        "activezones": activezones_results
    }

    return formated_records

def __format_current_datetime():
    now = datetime.now()
    current_datetime = now.strftime('%Y-%m-%d %H:%M:%S')

    return current_datetime

def __get_customer_data(
        file = 'c:\\programdata\\storage-tooling\\context_stack_names.csv',
        debug = False
    ):
    """Internal Function to get MTEST customer data from a file and add it to a deictionary
    Args:
        file (String, optional): The customer data file path and file name

    Object:
        Formatted customer data (Dictionary): Customer dictionary file
        with data formatted from MTEST
    """
    log_entry = {}

    try:
        with open(file, 'r', encoding="utf-8") as file_data:
            dict_reader = DictReader(file_data)
            customer_dict = list(dict_reader)
    except ResourceWarning:
        print(f'ERROR: Reading csv file {file} into dictionary')

    # Debug/Toubleshooting data
    if debug:
        log_entry = {
            "customer_dict": customer_dict
        }

    debug_log.append(log_entry)

    return customer_dict

def __get_runid(conn):
    """Internal Function to INSERT a new RUN table record and then retrieve the RunID for the run
    Args:
        conn (Pyodbc connection object, required): The established SQL connection object
        that has connected to the desired database

    Object:
        RunID (Dictionary): RunID result dictionary
    """

    results = []

    cursor = conn.cursor()

    # Format current time for use in creting Run record
    formatted_datetime = __format_current_datetime()

    # Attempt to create new Run record in Run table in storagetooling DB
    try:
        formatted_run = {'capturedatetime': formatted_datetime}
        insert_data_to_table('run', formatted_run, conn)
    except ResourceWarning:
        print('ERROR: Creting Run record Run table. Trminating program.')
        sys.exit()

    # Attempt to read newly created Run record and get Run ID
    try:
        query = f'SELECT [ID] FROM [{Default_Sql_Connection_Values["Database"]}] +\
        .[dbo].[RUN] WHERE capturedatetime Like "{formatted_datetime}%"'
        query_results = cursor.execute(query)

    except ResourceWarning:
        print('Error retrieving ID value for run in Run table. Terminating program.')
        sys.exit()

    # Convert data row format of query to a dictionary record
    columns = [column[0] for column in cursor.description]

    for row in query_results.fetchall():
        results.append(dict(zip(columns, row)))

    return results

def __get_server_data(
        password = "",
        url = 'https://sannav101.corp.athenahealth.com/external-api/v1/login/',
        username = 'svc-sannavread'
    ):

    """Internal Function to connect to SanNav API and retreieve switchporta records
    Args:
        password (String,required): The password name to use for connecting to SQL.

        url (String, optional): The SanNav API URL path.
        The default is https://sannav101.corp.athenahealth.com/external-api/v1/login/.

        username (String, optional): The user name to use for connecting to SQL.
        The default is the SanNav local user svc-sannavread.

    Object:
        SanNav data (List): A list of returned SanNav switchports data
    """

    # Determine if password was passed and exit if password is blank
    if password == "":
        raise ValueError("Password blank. Teminating function.")

    # Attempt to login to SanNAv and retrieve a session key. Terminate if login fails
    try:
        session_key_result = dbquery.__connect_to_sannav(username, password, url)
        storage_data_object.session_key = session_key_result['session_key']
    except ResourceWarning:
        print('ERROR: Session Key retrieval issue')

    if session_key_result['error'] is True:
        raise ValueError(f"Session key error data: {(session_key_result['error_data'])}")

    # Make call to retrieve switchport data from SanNav using the retrieved session key
    try:
        switchport_lookup_results =  dbquery.__query_sannav(
            "https://sannav101.corp.athenahealth.com/external-api/v1/inventory/switchports/?basicOnly=0",
            storage_data_object.session_key
        )
    except ResourceWarning:
        print('ERROR: SanNav switchports lookup')

    # Logout of SanNav and close session
    try:
        dbquery.__logout_sannav(
            storage_data_object.session_key,
            url = "https://sannav101.corp.athenahealth.com/external-api/v1/logout/"
        )

        storage_data_object.session_key = None
    except ResourceWarning:
        print('ERROR: SanNav logout')

    return switchport_lookup_results

def __handle_hierarchy_id(value):
    """Internal Function to provide string conversion for Type 151: SQL_SS_UDT columns for pyodbc.
    Added to connextion object to provide automatic handling of error.

    Args:
        value (String, required): The name of the SQL column data value


    Object:
        Column value (String): The passed value as type String
    """

    return str(value)

def __handle_datetimeoffset(date_time_offset_value):
    """Internal function to provide datetime conversion for Type 155: SQL_SS_TIMESTAMPOFFSET columns
       for pyodbc. Added to connextion object to provide automatic handling of error.

    Args:
        date_time_offset_valu (String, required): The name of the SQL column data value


    Object:
        Converted datetime (String): The passed value as type String
    """

    # Unpack datetime with time offset
    try:
        converted_date = struct.unpack("<6hI2h", date_time_offset_value)
    except ResourceWarning:
        print('ERROR: Performing datetime offset data translation')

    # Format and return unpacked datetime data as string
    return str(
        f'{converted_date[0]}-{converted_date[1]}-{converted_date[2]} +\
        {converted_date[3]}:{converted_date[4]}:{converted_date[5]}.{converted_date[6]}'
    )

def __read_sql_tables_to_cache(
        table_name,
        conn
    ):
    """Internal Function to read the passed table's records into local cache object
    Args:
        table_name (String, required): The table name to use for the INSERT to SQL.

        conn (Pyodbc connection object, required): The established SQL connection object
        that has connected to the desired database

    Object:
        none
    """

    results = []

    # Craft SQL query based on table name
    if table_name != 'activezones':
        query = f"SELECT * FROM [{Default_Sql_Connection_Values['Database']}].[dbo].[{table_name}]"
    else:
        query_suffix = "WHERE RunId = '{(storagetooling_tables_cache.run[(len(storagetooling_tables_cache.run)-1)])['ID']}'"
        query = f"SELECT * FROM [{Default_Sql_Connection_Values['Database']}].[dbo].[{table_name}] {query_suffix}"

        print(f"ActiveZones table query: {query}.")

    cursor = conn.cursor()

    # Attempt to SELECT records in the table passed and convert the results from datarows to a list
    try:
        query_results = cursor.execute(query)
        columns = [column[0] for column in cursor.description]
    except ResourceWarning:
        print('ERROR: MS SQL connection negotiation issue')

    # Add the results list to the storagetooling_tables_cache class under the table name attribute
    for row in query_results.fetchall():
        results.append(dict(zip(columns, row)))

    setattr(storagetooling_tables_cache, table_name, results)

def __update_source_refresh_timestamp(
        data_label,
        conn
    ):
    """Internal Function to UPDATE the UpdateTracking table to record a refresh to database
    Args:
        data_label (String, required): The table name to use for the INSERT to SQL.

        conn (Pyodbc connection object, required): The established SQL connection that has connected
        to the desired database

    Object:
        none
    """

    cursor = conn.cursor()

    current_datetime = __format_current_datetime()

    update_statement = f'UPDATE [dbo].[UpdateTracking] SET [{data_label}] = "{current_datetime}"'

    # Attempt to UPDATE record into database
    try:
        cursor.execute(update_statement)
        conn.commit()
        print(f'Success: {update_statement} run successfully.')
    except ResourceWarning:
        print(f'Error: Attempting to run {update_statement}')

async def __sannav_lookup_async(
        runid,
        **kwargs
    ):
    """Internal Function for creating a list of concurrent stack runs for each stack and run jobs.

    Args:
        stack_object (List, required): The list object with all the DNS records to query

    Object:
        Stack lookup result object(List): List of each stack DNS lookup result object
    """
    coros = None
    returned_exceptions = kwargs.pop('return_exceptions', True)

    coros = [
        __format_sannav_insert_data(sannav_object, runid)
        for sannav_object in storage_data_object.sannav
    ]

    return await asyncio.gather(*coros, return_exceptions = returned_exceptions)

def get_all_storage_data(
        sannav_password,
        sannav_user = '',
        debug = False
    ):
    """Wrapper Function for retrieving all external source data.

    Args:
        password (String, required): The password name to use for connecting to SanNav API.

        username (String, optional): The user name to use for connecting to SanNav API.
        The default is blank.

    Object:
        none
    """

    # Attempt to retrieve Customer data and add the output to Storage_Data class object
    try:
        storage_data_object.customers = __get_customer_data(debug = debug)
    except ResourceWarning:
        print('ERROR: Retrieving athenaOne customer data')

    # Attempt to retrieve Webpool/Webs stack data and add the output to Storage_Data class object
    try:
        storage_data_object.stacks = stackdns.get_stacks_dns_data()
    except ResourceWarning:
        print('ERROR: Retrieving stack (Webpool/Webs) data')

    # Attempt to retrieve SanNav switchport data managing login, query, and logout
    try:
        if sannav_user == '':
            sannav_results = __get_server_data(password = sannav_password)
        elif sannav_user != '':
            sannav_results = __get_server_data(password = sannav_password, username = sannav_user)
    except ResourceWarning:
        print('ERROR: Retrieving SanNav switchport data')

    # Add the output to Storage_Data class object
    storage_data_object.sannav = sannav_results["return_object"]["switchPorts"]

def get_storage_data_properties():
    """Function to display the current storage_data_object property values.

    Args:
        none

    Object:
        none
    """
    try:
        temp = storage_data_object.__dict__
    except ResourceWarning:
        print("ERROR: Displaying storage_data_object properties")

    return temp

def insert_data_to_table(
        table_name,
        record_dict,
        conn
    ):
    """Internal Function to INSERT a new record to the database using the passed connection.

    Args:
        table_name (String, required): The table name to use for the INSERT to SQL.

        record_dict (Dictionary, required): Dictionary file with data to craft the INSERT.

        conn (Pyodbc connection object, required): The established SQL connection connected
        to the desired database

    Object:
        none
    """

    cursor = conn.cursor()

    column_insert = []
    value_insert = []

    # Retrieve list of column names from passed record dictionary
    columns_list = list(record_dict.keys())

    # Format the column and value field and build the insert statement
    for column in columns_list:
        if column not in ('datecreated', 'ID'):
            column_insert.append((f"[{column}]"))
            try:
                if "'" in record_dict[column]:
                    record_dict[column] = (record_dict[column]).replace("'","''")

                value_insert.append((f"'{record_dict[column]}'"))
            except ResourceWarning:
                print(f'ERROR: Retrieving {record_dict[column]} key value from record')

    formated_column_insert = ",".join(column_insert)
    formated_value_insert = ",".join(value_insert)
    insert_statement = f'INSERT INTO [dbo].[{table_name}] ({formated_column_insert}) VALUES({formated_value_insert})'

    # Attempt to INSERT record into database
    try:
        cursor.execute(insert_statement)
        conn.commit()
        print(f'Success: {insert_statement} run successfully.')
    except ResourceWarning:
        print(f'ERROR: Attempting to run {insert_statement}')

def insert_data_to_table_batch(
        table_name, record_list,
        conn,
        tranaction_block_size = 250
    ):

    """Internal Function to INSERT a block of records to the database using the passed connection
    Args:
        table_name (String, required): The table name to use for the INSERT to SQL.

        record_list (List of Dictionaries, required): Dictionary file with data to craft the INSERT.

        conn (Pyodbc connection object, required): The established SQL connection that has connected
        to the desired database

        tranaction_block_size (Integer, optional): Number of inserts to be batched as one insert.
        The default value is 250

    Object:
        none
    """

    start_time = time.time()

    cursor = conn.cursor()
    count = 0

    insert_stement_block = str()
    column_insert = []

    # Retrieve list of column names from passed record dictionary
    columns_list = list(record_list[0].keys())

    # Format the column fields and create the base INSERT statement
    for column in columns_list:
        if column not in ('datecreated', 'ID'):
            column_insert.append((f"[{column}]"))
            # try:
            #     if "'" in record_list[0][column]:
            #         # Likelyreplaced with  record_list[0][column] = (record_list[0][column]).replace("'","''") Follow up later
            #         entry[column] = (record_list[0][column]).replace("'","''")
            # except Exception as err:
            #     print(f'Error retrieving {record_list[0][column]} key value from record:{err}')

    formated_column_insert = ",".join(column_insert)
    insert_stement_block = f"INSERT INTO [dbo].[{table_name}] ({formated_column_insert}) VALUES\r\n"

    # Format the batched Value statements based on passed block size
    # and join to the base INSERT statement
    for entry in record_list:
        value_insert = []
        formated_value_insert = str()

        # Create each row of values for inserting
        for column in columns_list:
            if column not in ('datecreated', 'ID'):
                try:
                    value_insert.append((f"'{entry[column]}'"))
                except ResourceWarning:
                    print(f'ERROR: Retrieving {entry[column]} key value from record')

        formated_value_insert = ",".join(value_insert)
        insert_stement_block += f"({formated_value_insert}),\r\n"
        count += 1

        # Determine when the passed block size has been reached and commit block of inserts
        if (count % tranaction_block_size == 0) and (count != 0):
            try:
                insert_stement_block = insert_stement_block[:-3]
                #debug_log.append({'inserts': insert_stement_block})
                cursor.execute(insert_stement_block)
                conn.commit()
            except ResourceWarning:
                print(f'ERROR: Running insert transaction: {insert_stement_block}')

            insert_stement_block = f'INSERT INTO [dbo].[{table_name}] ({formated_column_insert}) VALUES\r\n'
            count = 0

    # Process any fractional amount of inserts once the end of the record list is reached
    # and commit the inserts
    if count > 0:
        try:
            insert_stement_block = insert_stement_block[:-3]
            #debug_log.append({'inserts': insert_stement_block})
            cursor.execute(insert_stement_block)
            conn.commit()
        except ResourceWarning:
            print(f'Error: Running INSERT transaction: {insert_stement_block}.')

    finish_time = (time.time()) - start_time
    print(f'Total Execution Time for for {table_name} batched transaction INSERTs: {finish_time}')

def update_customer_data(conn):
    """Function for processing of the MTEST customer records dump file .

    Args:
        conn (Pyodbc connection object, required): The established SQL connection object
        that has connected to the desired database

    Object:
        none
    """

    # Attempt to read the customer data file from MTEST
    try:
        results = asyncio.run(__customer_lookup_async())
    except ResourceWarning:
        print('ERROR: Formatting customer data for review')

    # Call function to clear customers table in database
    delete_status_customer = __clear_table('customers', conn)

    # Call function to process and update customer data if the customers table was cleared
    if not delete_status_customer:
        sys.exit("No update to customers table due to table clear failure.")

    # Process the file if records exist, insert them into the database,
    # and reload customers table to cache
    if len(results) > 0:
        try:
            insert_data_to_table_batch('customers',  results, conn)
            __read_sql_tables_to_cache('customers', conn)
        except ResourceWarning:
            print('ERROR: Updating customer data')

        # Refresh UpdateTracking time for customers column
        try:
            __update_source_refresh_timestamp('customers', conn)
        except ResourceWarning:
            print('ERROR: Logging customer data refresh timestamp')
    else:
        print("No customer data was found to process in modules customers class attribute.")

def update_data_table(
        table_name,
        record_dict,
        conn
    ):
    """Function for processing UPDATE statement to database.

    Args:
        table_name (string, required): The name of the table to run UPDATE on.

        record_dict (Dictionary, required): The dictionary with the data to use for UPDATE

        conn (Pyodbc connection object, required): The established SQL connection object
        that has connected to the desired database

    Object:
        none
    """
    cursor = conn.cursor()

    record_update = []
    update_stement = f'UPDATE [dbo].[{table_name}]\r\nSET '

    for column in record_dict['updates']:
        record_update.append(f"[{column}] = '{record_dict['updates'][str(column)]}'\r\n")

    formated_updates = ",".join(record_update)
    update_stement += f"{formated_updates}WHERE {record_dict['where_clause']}"

    # Attempt to update record in database
    try:
        cursor.execute(update_stement)
        conn.commit()
        print(f'Success: {update_stement} run successfully.')
    except ResourceWarning:
        print(f'ERROR: Attempting to run {update_stement}')

def update_stack_data(
        conn,
        debug = False
    ):
    """Function for processing of the Stack and Webpool records from DNS lookups.

    Args:
        conn (Pyodbc connection object, required): The established SQL connection object
        that has connected to the desired database

    Object:
        none
    """

    # Loop through the satck data in the storage_data_object Class cache,
    # determine if record already exists in table, and INSERT record if needed
    for record in storage_data_object.stacks:
        # Set type to None to clear old data if module is run multiple times in memory space
        stacks_data_formatted = None
        update_info = None
        webpools_data_formatted = None
        webs_data_formatted = None
        webs_list = None

        # Define required dictionaries
        log_entry = {}
        stacks_data_formatted = {}
        update_info = {}
        webpools_data_formatted = {}
        webs_data_formatted = {}

        webpool_location_check = ['prod', 'remote']

        # Create Stack portion of the record
        try:
            stacks_data_formatted = {
                'Stack': str(record['stack']),
                'locallocation': record['local_location'],
                'prodlocation': record['prod_location'],
                'remotelocation': record['remote_location']
            }
        except ResourceWarning:
            print_values = f"{record['stack']},  {record['local_location']}, {record['prod_location']}, {record['remote_location']}"
            print(f"ERROR: Attempting to build stack dictionary. Values: {print_values}")

        # Determine if stack already exists in database by checking table cache
        search_stack_table = [
            item for item in storagetooling_tables_cache.stacks
            if item['Stack'] == record['stack']
        ]

        # If stack was not found, INSERT record into the database and refresh stacks table cache
        if not search_stack_table:
            try:
                insert_data_to_table('stacks', stacks_data_formatted, conn)
                __read_sql_tables_to_cache('stacks', conn)
            except ResourceWarning:
                print('ERROR: Updating stacks data')
        else:
            # Check record and update each column as needed
            update_needed = False
            stacks_columns_update = {}
            table_entry = search_stack_table[0]

            if stacks_data_formatted['locallocation'] != table_entry['locallocation']:
                stacks_columns_update['locallocation'] = stacks_data_formatted['locallocation']
                update_needed =  True

            if stacks_data_formatted['prodlocation'] != table_entry['prodlocation']:
                stacks_columns_update['prodlocation'] = stacks_data_formatted['prodlocation']
                update_needed =  True

            if stacks_data_formatted['remotelocation'] != table_entry['remotelocation']:
                stacks_columns_update['remotelocation'] = stacks_data_formatted['remotelocation']
                update_needed =  True

            stacks_where_clause = f"stack = '{str(record['stack'])}'"

            update_info = {
                'updates': stacks_columns_update,
                'where_clause': stacks_where_clause
            }

            if update_needed:
                update_data_table('stacks', update_info, conn)


        # Loop through the webpool_location_check, cretate location URLs, determine if record exists in table,
        # and INSERT record if needed
        for location in webpool_location_check:
            webpool_name = f'{location}_webpool_name'
            webpool = f'{location}_webpool'

            # Create webpool portion of the record
            try:
                webpools_data_formatted = {
                    'name': record[str(webpool_name)],
                    'stackid': str(record['stack'])
                }

            except ResourceWarning:
                print_values = f"{record[str(webpool_name)]}, {record['stack']}"
                print(f"ERROR: Attempting to build webpool dictionary. Values: {print_values}")

            # Determine if webpool already exists in database by checking table cache
            search_webpools_table = [
                item for item in storagetooling_tables_cache.webpools
                if item['name'] == record[str(webpool_name)]
            ]

            # If webpool was not found, INSERT record into database and refresh webpools table cache
            if search_webpools_table:
                webpoolid = str(search_webpools_table[0]["ID"])
            else:
                try:
                    insert_data_to_table('webpools', webpools_data_formatted, conn)
                    __read_sql_tables_to_cache('webpools', conn)
                except ResourceWarning:
                    print("ERROR: Updating webpool data")

                # Search through refreshed cache to get new webpool ID value
                search_webpools_table = [
                    item for item in storagetooling_tables_cache.webpools
                    if item['name'] == record[str(webpool_name)]
                ]

                webpoolid = str(search_webpools_table[0]["ID"])

            # Format webs as list to iterate through
            webs_list = (record[str(webpool)]).split(',')

            # Loop through the web list, determine if record already exists in table,
            # and INSERT record if needed
            for web in webs_list:
                # Create webpool portion of the record
                try:
                    webs_data_formatted = {'name': web, 'webpoolid': webpoolid}
                except ResourceWarning:
                    print_values = f"{web}, {webpoolid}"
                    print(f"Error trying to build formated web dictionary. Values: {print_values}")

                # Search table cache and determine if web value already exists
                search_webs_table = [
                    item for item in storagetooling_tables_cache.webs
                    if item['name'] == web
                ]

                # If web was not found, INSERT record into the database
                if not search_webs_table:
                    try:
                        insert_data_to_table('webs', webs_data_formatted, conn)
                    except ResourceWarning:
                        print("ERROR: Updating webs data")

            # Refresh webs table cache
            __read_sql_tables_to_cache('webs', conn)
            # Still need to handle updates for stacks, webpools and webs

        # Debug/Toubleshooting data
        if debug:
            log_entry = {
                "stacks_data_formatted": stacks_data_formatted,
                "update_info": update_info,
                "webpools_data_formatted": webpools_data_formatted,
                "webs_data_formatted": webs_data_formatted,
                "webs_list": webs_list
            }

        debug_log.append(log_entry)

    # Refresh UpdateTracking time for stacks column
    try:
        __update_source_refresh_timestamp('stacks', conn)
    except ResourceWarning:
        print("ERROR: Logging stack data refresh timestamp")

def update_sannav_data(
        conn,
        debug = False
    ):
    """Function for processing of the SanNav switchports data retrieved from SanNav API.

    Args:
        conn (Pyodbc connection object, required): The established SQL connection object
        that has connected to the desired database

    Object:
        SanNav records (List of Dictionaries): The returned formatted SanNav data
    """

    start_time_all = time.time()

    sannav_common_id_tables = [
        'activeZones',
        'fabric',
        'health',
        'status',
        'switch',
        'zone'
    ]

    # Set type to None to clear old data if module is run multiple times in memory space
    log_entry = {}
    sannav_formatted = None
    temp_activezones_formatted = None
    activezones_formatted = None
    sannav_object_insert_dict_list = None
    activezone_object_insert_dict_list = None
    runid = None

    # Define required dictionaries
    activezones_formatted = []
    sannav_object_insert_dict_list = []
    activezone_object_insert_dict_list = []

    start_time_id_tables = time.time()

    # Add none values for attributes that may be missing in some records
    for item in storage_data_object.sannav:
        if 'zoneAlias' not in item.keys():
            item['zoneAlias'] = 'none'
        if 'entitytype' not in item.keys():
            item['entitytype'] = 'none'
        if 'activeZones' not in item.keys():
            item['activeZones'] = ['none']
        if 'remoteNodeWwn' not in item.keys():
            item['remoteNodeWwn'] = 'none'

    # Loop through defined common ID tables determine if record already exists in database,
    # and INSERT record if needed
    for table in sannav_common_id_tables:
        # Format source to match the SanNav formatted parameter name
        if table in ('switch', 'fabric'):
            source = f"{table}Name"
        elif table == 'zone':
            source = f"{table}Alias"
        else:
            source = table

        # Determine if table record exists, if not INSERT record, and then refresh cache
        if table != 'activeZones':
            unique_source_values = set(map(itemgetter(source), storage_data_object.sannav))

            for record in unique_source_values:
                search_table = [
                    item for item in getattr(storagetooling_tables_cache, table)
                    if item['name'] == record
                ]

                data_formatted = {'name': record}

                if not search_table:
                    insert_data_to_table(table, data_formatted, conn)

            __read_sql_tables_to_cache(table, conn)
        else:
            for record in storage_data_object.sannav:
                if 'activeZones' in record.keys():
                    activezone_object_id_table_dict_list.append(record['activeZones'])

            unique_source_values = {
                entry for sub_list in activezone_object_id_table_dict_list
                for entry in sub_list
            }

            for record in unique_source_values:
                search_table = [
                    item for item in getattr(storagetooling_tables_cache, 'zone')
                    if item['name'] == record
                ]

                data_formatted = {'name': record}

                if not search_table:
                    insert_data_to_table('zone', data_formatted, conn)

            __read_sql_tables_to_cache('zone', conn)

    finish_time_id_tables = (time.time()) - start_time_id_tables
    print(f"Total Execution Time inserting table values into ID tables: {finish_time_id_tables}")

    # Make request for new Run entry and retrieve the new RunId
    get_runid = __get_runid(conn)
    runid = f"{get_runid[0]['ID']}"

    start_time_sannav_format = time.time()

    # Asyncrhonous processing/formatting of SanNav data, including activezones
    results = asyncio.run(__sannav_lookup_async(runid))

    # Extract returned data for sannaventities format records from combined (sannav/activezone) data
    sannav_formatted = [d['sannav'] for d in results]

    # Create list for the extracted sannav data
    for device_entry in sannav_formatted:
        sannav_object_insert_dict_list.append(device_entry)

    # Extract returned data for activezones formatted records from combined (sannav/activezone) data
    temp_activezones_formatted = [d['activezones'] for d in results if len(d['activezones']) > 0]

    # Create list for the extracted activezone data
    for list_entry in  temp_activezones_formatted:
        for dict_entry in list_entry:
            activezones_formatted.append(dict_entry)

    for activezone_entry in activezones_formatted:
        activezone_object_insert_dict_list.append(activezone_entry)

    finish_time_sannav_format = (time.time()) - start_time_sannav_format
    print(f"Total Execution Time for retrieving formatted SanNav data: {finish_time_sannav_format}")

    finish_time_all = (time.time()) - start_time_all
    print(f"Total execution Time formating SanNav data for batch INSERT: {finish_time_all}")

    print("Initiating activeZones table insert tranaction commits.")

    # If new activezones record need to be added, INSERT them into database
    if len(activezone_object_insert_dict_list) != 0:
        insert_data_to_table_batch('activezones', activezone_object_insert_dict_list, conn)
        __read_sql_tables_to_cache('activezones', conn)

    print("Initiating sannavEntities table insert tranaction commits.")

    # If new sannaventities record need to be added, INSERT them into database
    if len(sannav_object_insert_dict_list) != 0:
        try:
            insert_data_to_table_batch('sannaventities', sannav_object_insert_dict_list, conn)
        except ResourceWarning:
            print("ERROR: Updating sannaventities data")

    # Refresh UpdateTracking time for sutomer column
    try:
        __update_source_refresh_timestamp('sannaventities', conn)
    except ResourceWarning:
        print("ERROR: Logging sannaventities data refresh timestamp")

    # Debug/Toubleshooting data
    if debug:
        log_entry = {
            "results": results,
            "activezone_formatted": temp_activezones_formatted,
            "sannav_formatted": sannav_formatted,
            "activezones_inserts": activezone_object_insert_dict_list,
            "sannav_inserts": sannav_object_insert_dict_list
        }

        debug_log.append(log_entry)

def update_data_from_sources(
        username = '', password = '',
        db_server = Default_Sql_Connection_Values["Server"],
        database = Default_Sql_Connection_Values["Database"],
        driver = Default_Sql_Connection_Values["Driver"],
        trust_certificates = Default_Sql_Connection_Values["TrustServerCertificate"],
        sannav_password = '',
        debug = False,
        automated = False):

    """Function to load all required data from external sources for review, formating, and updates.

    Args:
        username (String, optional): The user name to use for connecting to SQL.
        The default is blank and will instead use the current username of the initiating user.

        password (String, optional): The password name to use for connecting to SQL.
        The default is blank and will instead use the current credentials of the initiating user.

        db_server (String, optional): The DNS name of the sql database server to connect to.
        The default value is the global dictionary Default_Sql_Connection_Values["Server"] value.

        database (String, optional): Name of the sql database to connect to the targeted SQL server.
        The default value is the global dictionary Default_Sql_Connection_Values["Database"] value.

        driver (String, optional): The locally installed ODBC DataSource Drive name to be used on
        the mahcine wher the code is running from.
        The default value is the global dictionary Default_Sql_Connection_Values["Driver"] value.

        trust_certificates (String, optional): Flag value to trust unverified certificates (yes/no).
        The default value is global dictionary Default_Sql_Connection_Values["TrustServerCertificate"].

        sannav_password (String, optional): The password name to use for connecting to SQL.
        The default is blank.


    Object:
        SQL connection objet (Object): An active connection object used to initiate connections
        and send SQL commands for execution
    """

    # Determine if this is being run from Jenkins and retrieve
    # the defined Jenkins credentials from the job
    if automated:
        if 'WinUsername' in os.environ:
            username = os.environ['WinUsername']
            if 'WinPassword' in os.environ:
                password = os.environ['WinPassword']
            else:
                message = f'No password is set in WinPassword for user: {username}. Terminating function update_data_from_sources.'
                sys.exit(message)

    # Establish connection with database that will be used by all other functions
    sql_connection = __connect_ms_sql(
        username,
        password,
        db_server,
        database,
        driver,
        trust_certificates
    )

    print(sql_connection)

    start_time_update_data_all = time.time()

    print("Initiating ID query tables into local cache.")
    start_time_cache_load = time.time()

    # Call function to read ID tables to cache class object storagetooling_tables_cache
    for table in Storage_Id_Tables:
        try:
            __read_sql_tables_to_cache(table, sql_connection)
        except ResourceWarning:
            print(f"ERROR: Reading the {table} table into cache")

    print("Initiating Core query tables into local cache.")

    # Call function to read Core tables to cache class object storagetooling_tables_cache
    for table in Storage_Core_Tables:
        try:
            __read_sql_tables_to_cache(table, sql_connection)
        except ResourceWarning:
            print(f"ERROR: Reading the {table} table into cache")

    finish_time_cache_load = (time.time()) - start_time_cache_load
    print(f"Total execution time for caching ID tables: {finish_time_cache_load}")

    print("Initiating external source data retrieval (MTEST, DNS, and SanNav).")
    start_time_retrieve_source_data = time.time()

    # Call function to retrieve all external source data for Stack/Web, Customers, and SanNav
    get_all_storage_data(sannav_password = sannav_password, debug = debug)

    finish_time_retrieve_source_data = (time.time()) - start_time_retrieve_source_data
    print(f"Total Execution Time for data retrieval (MTEST, DNS, and SanNav): {finish_time_retrieve_source_data}")

    print("Initiating update of stack data in the storagetooling database.")
    start_time_updating_stack_data = time.time()

    # Call function to process and update Stacks, Webpools, and Webs data
    update_stack_data(sql_connection, debug)

    finish_time_updating_stack_data = (time.time()) - start_time_updating_stack_data
    print(f"Total Execution Time to update stack data in the storagetooling database: {finish_time_updating_stack_data}")

    print("Initiating update of customer data in the storagetooling database.")
    start_time_updating_customer_data = time.time()

    # Call function to process and update customer data
    update_customer_data(sql_connection)

    finish_time_updating_customer_data = (time.time()) - start_time_updating_customer_data
    print(f"Total Execution Time to update customer data in the storagetooling database: {finish_time_updating_customer_data}")

    print("Initiating SanNav data run update to storagetooling database.")
    start_time_updating_sannav_data = time.time()

    # Call function to process SanNav data
    update_sannav_data(sql_connection, debug)

    finish_time_updating_sannav_data = (time.time()) - start_time_updating_sannav_data

    print(f"Total Execution Time for updating SanNav data run update to storagetooling database: {finish_time_updating_sannav_data}")

    finish_time_update_data_all = (time.time()) - start_time_update_data_all
    print(f"Total Execution Time for all steps to update the storagetooling database: {finish_time_update_data_all}")

def update_session_key(session_key):
    """Function to update session key for storage_data_object.

    Args:
        sessionkey (String, required): Session key to update session_key property in storage_data_object.

    Object:
        none
    """
    try:
        storage_data_object.session_key = session_key
        print("SUCCESS: Session Key Stored")
    except ResourceWarning:
        print("ERROR: Storing Session Key")
