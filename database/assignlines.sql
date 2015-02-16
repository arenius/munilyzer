UPDATE runs SET route_group = line_name
FROM
	(
	WITH 
	runsextract AS -- Returns start or end points of a run.  One per row.
		( 
		SELECT runs.rid, gpspoints.oid, gpspoints.vehicle_id, gpspoints.timedate,
		        runs.run_block
		FROM gpspoints, runs
		WHERE (gpspoints.oid = runs.start_point_oid OR
			gpspoints.oid = runs.end_point_oid)
		),
	runendpoints AS -- Returns the start and end points of a run as a single row
		( 
		SELECT vehicle_id, run_block, min(timedate), max(timedate)
		FROM runsextract
		GROUP BY vehicle_id, run_block
		),
	pointswithruns AS -- Returns all points with their respective run blocks.
		(
		SELECT gpspoints.oid, runendpoints.vehicle_id, gpspoints.timedate,
		        runendpoints.run_block, gpspoints.geom
		FROM gpspoints, runendpoints
		WHERE gpspoints.vehicle_id = runendpoints.vehicle_id AND
		      /*This OVERLAPS statement restricts result to points that have a timedate in 
		      the correct interval.*/
		      (timedate,timedate) OVERLAPS (min,max)
		),
	distance AS -- Returns distance from each point to every bus route.
		(
		SELECT ST_distance(pointswithruns.geom, munirts_simple2.geom) AS distance,
		        line_name, vehicle_id, run_block
		FROM pointswithruns, munirts_simple2
		WHERE dow_to_servicegro() = servicegro 
		      -- AND vehicle_id = '5552' --UNCOMMENT TO TEST ON SMALL SECTION OF DB 
		),
	avgdist AS -- Returns the average distance for all points in a run block to every bus route
		(
		SELECT vehicle_id, run_block, avg(distance) AS average_distance, line_name
		FROM distance
		GROUP BY vehicle_id, run_block, line_name
		),
	minavgdist AS /* Returns the smallest average distance between the points of a run block
		    and every bus route.*/
		(
		SELECT vehicle_id, run_block, min(average_distance) AS minimum_average_distance
		FROM avgdist
		GROUP BY vehicle_id, run_block
		) 
	SELECT avgdist.line_name, minavgdist.minimum_average_distance, avgdist.run_block,
	        avgdist.vehicle_id		
	FROM
		minavgdist
		JOIN
		avgdist
		ON minavgdist.vehicle_id = avgdist.vehicle_id 
			AND minavgdist.run_block = avgdist.run_block 
			AND minimum_average_distance = average_distance
	) AS sq5
WHERE runs.vehicle_id = sq5.vehicle_id::text AND runs.run_block = sq5.run_block
;
