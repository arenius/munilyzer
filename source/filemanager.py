#This module searches for new files to download, adds the filename to the files table of the 
#database, attempts to download the file, and updates the files table if successful.

import psycopg2
import logging
import configparser
from ftplib import FTP
from datetime import datetime

logging.basicConfig(level='DEBUG')

config = configparser.ConfigParser()
config.read('munilyzer.ini')

ftp_address = config['ftp']['ftp_address']
ftp_directory = config['ftp']['ftp_directory']
database_string = config['database']['database_string']
download_directory = config['ftp']['download_directory']

def identify_new_files():
    """Check the ftp server for new files."""
    with psycopg2.connect(database_string) as conn:
        #Retrieve a list of which files we already have.
        logging.debug("Connected to database")
        with conn.cursor() as cur:
            local_files_list = []
            cur.execute('SELECT file_name FROM files')
            for row in cur:
                local_files_list.append(row[0])
            #Download raw gps data as .csv files from ftp server.
            #Connect to ftp server.
            logging.debug("Address of ftp server: " + ftp_address)
            with FTP(ftp_address) as ftp:
                ftp.login()
                ftp.cwd(ftp_directory)
                logging.debug("Logged into ftp server at:" + ftp_address)
                #Retrieve a list of files on the server.
                ftp_files_list = ftp.nlst()
                #For each file on the server, compare to the list of local files.
                for file_name in ftp_files_list:
                    #If the file isn't known about locally add it to files table.
                    if file_name not in local_files_list:
                        #Add filename to database.
                        cur.execute('INSERT INTO files VALUES (%s,%s,%s,%s)',
                                    (file_name,None,None,None))

def download_files():
    """Download files from the ftp server."""
    with psycopg2.connect(database_string) as conn:
        logging.debug("Connected to database")
        with FTP(ftp_address) as ftp:
            ftp.login()
            logging.debug("Logged into ftp server at: " + ftp_address)
            ftp.cwd(ftp_directory)
            with conn.cursor() as cur:
                #Get list of files that aren't yet downloaded.
                cur.execute('SELECT file_name FROM files WHERE downloaded IS NULL')
                files_to_download_list = cur.fetchall()
                for file_name in files_to_download_list:
                    #file_path is the full path that the new file is going to be downloaded to.
                    file_path = download_directory + file_name[0]
                    logging.debug("Attempting to download: " + file_name[0])
                    ftp.retrbinary("RETR " + file_name[0] , open(file_path, 'wb').write)
                    logging.debug("Successfully downloaded: " + file_name[0])
                    #If our download was successfull, update downloaded field
                    logging.debug(cur.mogrify('UPDATE files SET downloaded = %s ' 
                                              'WHERE file_name = %s', 
                                              (datetime.utcnow(),file_name[0]))
                                  )
                    cur.execute('UPDATE files SET downloaded = %s WHERE file_name = %s',
                                (datetime.utcnow(),file_name[0]))
                    conn.commit()

def clean_file(file_name):
    file_path = download_directory + file_name
    #line_list is a list of lines that are going to go in the cleaned .csv file.
    line_list = []
    #The first 30 characters of any line SHOULD be usable as unique keys, but some sometimes they
    #aren't. key_set is used to throw out the duplicates, of which there are ~1 per 20,000 lines.
    key_set = set()
    with open(file_path, 'r+') as f:
        for line in f:
            #if the key isn't in the key set and the line is not blank, keep it.
            if (line[:29] not in key_set) and (line != '\n'):
                #Skip the first 5 characters, as they aren't needed.
                line_list.append(line[:5])
                key_set.add(line[:29])
        #Remove the header data. The header data in these files is only half
        #of the first line; the rest is data.
        line_list[0] = line_list[0][89:]
        #Write clean data to file
        f.writelines(line_list)

def file_name_check(file_name):
    """Ensure that the file we are cleaning is one we should clean."""
    if (file_name[:15] == "sfmtaAVLRawData" and
        file_name[-4:] == ".csv"):
        return True
    else:
        return False

def get_files_to_clean():
    with psycopg2.connect(database_string) as conn:
        logging.debug("Connected to database")
        with conn.cursor() as cur:
            cur.execute('SELECT file_name FROM files WHERE cleaned IS NULL '
                        ' AND downloaded IS NOT NULL ')
            files_to_clean_list = cur.fetchall()
            for file_name in files_to_clean_list:
                if file_name_check(file_name[0]):
                    clean_file(file_name[0])
                    logging.debug("Cleaned file: " + file_name[0])
                    cur.execute('UPDATE files SET cleaned = %s WHERE file_name = %s',
                                (datetime.now(),file_name[0]))
                    conn.commit()

if __name__ == "__main__":
    #identify_new_files()
    #download_files()
    get_files_to_clean()
