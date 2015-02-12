from os import walk
import psycopg2

database_info = 'String with dbname=dbname and user=user'
file_data_location = 'String with path to file_data file'
data_directory = 'String with path to our data directory'
table_name = 'String with name of gps point table in postgres'

#
def get_file_data(filename):
	file_data = []
	with open(filename, 'r') as f:
		file_data.extend(f)
        for index, line in enumerate(file_data):
                line = line.split(',')
                line = (line[0], bool(line[1]), bool(line[2]))
                file_data[index] = line
	return file_data

def check_for_new_data(directory):
	file_list = []
	new_file_list = []
	old_file_set = set()
	#Walk data directory for list of file names.
	for (dirpath, dirnames, filenames) in walk(directory):
		file_list.extend(filenames)
		break
	#Get set of files already loaded into DB.
	for line in file_data:
			old_file_set.add(line)
	#Check if files we just retrieved are already in DB.
	for name in file_list:
		if name not in old_file_set:
			new_file_list.append(name)
	return new_file_list
				
def clean_data():
        for file in new_files:
		gpsdata = []
		seensub = set()
		with open(filename, 'r') as f:
			for line in f:
				if (line[:29] not in seensub) and (line != '\n'):
					gpsdata.append(line[5:])
					seensub.add(line[:29])
		#Remove the header data. The header data in these files is only half
		#of the first line; the rest is data.
		gpsdata[0] = gpsdata[0][89:]
		#Write clean data to file
		with open(filename, 'w') as f:
			f.writelines(gpsdata)
		
def import_data_to_database(tablename):
	cur = conn.cursor()
	for file in new_files:
		cur.copy_to(file, tablename, sep=',', null='')
	cur.close()
	

#Retrieve stored data about our data files.
#Format is: (string filename)
file_data = get_file_data(file_data_location)
new_files = check_for_new_data(data_directory)

#Clean our new_files.
clean_data()

#Create DB session
conn = psycopg2.connect(database_info)

#Import our new_files.
import_data_to_database(table_name)

#Update file_data.
with open(file_data_locations, 'a') as f:
	for line in new_files:
		f.write(line)

#Create geometry columns.
with conn.cursor() as curs:
	curs.execute(
		"""UPDATE %s 
		SET geom = ST_Transform(ST_SetSRID(ST_MakePoint(longitude,latitude),4326),26943) 
		WHERE geom = NULL;""", (table_name,))

#calculate and update avgspeed

#update overminspeed
with conn.cursor() as curs:
	curs.execute(
		"""UPDATE %s 
		SET overminspeed = TRUE 
		WHERE overminspeed = NULL AND avgspeed > .6;""", (table_name,))

#run run analysis

#create run blocks

#assign lines

#end point analysis

#broken run block analysis

#break multi run block analysis




		








		

    

