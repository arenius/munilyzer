import psycopg2

def files_to_copy():
    file_list = []
    with psycopg2.connect(database_string) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT file_name FROM files WHERE cleaned = TRUE AND in_database = FALSE')
            for row in cur:
                file_path = download_directory + row[0]
                file_list.append(file_path)
    return file_list

def copy_files_to_database(file_list):
    with psycopg2.connect(database_string) as conn:
        with conn.cursor() as cur:
            for file_ in file_list:
                cur.copy_to(file_, "points", sep=',', null='')
