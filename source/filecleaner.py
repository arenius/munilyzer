import psycopg2

def clean_file(file_name):
    file_path = download_directory + file_name
    #line_list is a list of lines that are going to go in the cleaned .csv file.
    line_list = []
    #The first 30 characters of any line SHOULD be usable as unique keys, but some sometimes they
    #aren't. key_set is used to throw out the duplicates, of which there are ~1 per 20,000 lines.
    key_set = set()
    with open(filename, 'r+') as f:
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

def get_files_to_clean():
    with psycopg2.connect(database_string) as conn:
        with conn.cursor() as cur:
            files_to_clean_list = []
            cur.execute('SELECT file_name FROM files WHERE cleaned IS NULL')
            for row in cur:
                clean_file(row[0])
            cur.execute('UPDATE files SET cleaned IS NULL')
