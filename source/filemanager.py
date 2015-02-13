#Download Files
#add row to db.files for that file.
#clean files
#update db.files
#copy files to db.points
#update db.files

import psycopg2
import ftplib
import logging

ftp_address = "75.10.224.35"
ftp_directory = "/AVL_DATA/AVL_RAW/" 
database_string = "database connection string"
download_directory = "path/to/download/to/"

def download_files():
    """Check the ftp server for new files and download them."""
    with psycopg2.connect(database_string) as conn:
        #Retrieve a list of which files we already have.
        with conn.cursor() as cur:
            local_files_list = []
            cur.execute('SELECT file_name FROM files WHERE downloaded = FALSE')
            for row in cur:
                local_files_list.append(row[0])
            #Download raw gps data as .csv files from ftp server.
            #Connect to ftp server.
            with FTP(ftp_address) as ftp:
                ftp.login()
                ftp.cwd()
                #Retrieve a list of files on the server.
                ftp_files_list = ftp.nlst()
                #For each file on the server, compare to list of local files.
                for file_name in ftp_files_list:
                    #If the file isn't stored locally download it and add it to files table.
                    if file_name not in local_files_list:
                        #Add filename to database.
                        cur.execute('INSERT INTO files VALUES (?,?,?,?)',
                                    (file_name,False,False,False))
                        #file_path is the full path that the new file is going to be downloaded to.
                        file_path = download_directory + file_name
                        ftp.retrbinary("RETR " + file_name , open(file_path, 'wb').write)
                        #If our download was successfull, update downloaded field
                        cur.execute('UPDATE tables SET downloaded = TRUE')
                
            
                
