#This script attempts to determine if a bus is on a run or not at any given point.
#This is a first pass and does not need to determine that perfectly, and it doesn't.
#It functions by looking at the average speed for a given point, and the average speed for 
#the preceding two and following two points.  It looks at the pattern for those five 
#and uses that to determine if the bus is on a run.  To do that we have a pre-set list of
#patterns that are matched against to make that decision.

#run above with cur.execute to get list of tuples with average_speed as the only item in the tuples

#Check if there are four points that precede these un analyzed points.
#If there are, get them and prepend them to the list and start analysis on third point.
#If there aren't, assign on=false to the first two points if their avgspeed is under minspeed
#and on_run=True if their avgsppeed is over minspeed.

#Run analysis normally on rest of points except for last two, which should be done as same for
#first two.
#######################################
import psycopg2
from collections import deque

with pyscopg2.connect(database_string) as conn:

    #Retrieve list of vehicle_ids.
    def get_vehicle_ids():
        vehicle_id_sql = ('SELECT DISTINCT vehicle_id '
                          'FROM points ')
        with conn.cursor() as cur:
            cur.execute(vehicle_id_sql)
            vehicle_id_list = cur.fetchall()
            return vehicle_id_list

    #Execute Run Analysis.
    def run_analysis(get_vehicle_ids()):
        run_analysis_sql = ('SELECT point_id, average_speed '
                            'FROM points ' 
                            'WHERE on_run IS NULL AND vehicle_id = %s ')
        with conn.cursor() as cur:
            for id_ in vehicle_id_list:
                cur.execute(run_analysis_sql, id_[0])



#List of tuples to match against. Tuples that are in this set should be designated as on_run = True
on_run_patterns = []
#Speed below which bus movement is considered to be False.
min_speed = .6
#The pattern that will be matched against on_run_list. 
current_pattern = deque([],5)
#list of tuples (vehicle_id, average_speed) gathered from the database.
point_list = cur.fetchall
#list of speeds that the database will be updated with.
updated_point_list = []

#For each point in point_list:
for point in point_list:
    #If the average_speed of that point is greater than min_speed, leftappend(True) to the
    #current_pattern deque. Otherwise leftappend(False).
    if point[1] > min_speed:
        current_pattern.leftappend(True)
    else:
        current_pattern.leftappend(False)
    #If the current_pattern deque is in the on_run_patterns list,  
    if tuple(current_pattern) in on_run_list:
        updated_point_list.append(point[0],True)
    else:
        updated_point_list.append(point[0],False)




