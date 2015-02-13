#This module will populate the average_speed and geom columns of the database.

#For each row where there is no geometry, create one from the longitude, latitude columns.
with psycopg2.connect(database_string) as conn:
    with conn.cursor() as curs:
        curs.execute('UPDATE points'
                 'SET geom = ST_Transform(ST_SetSRID(ST_MakePoint(longitude,latitude),4326),26943)'
                 'WHERE geom IS NULL')





