CREATE TABLE points (
	oid 		BIGSERIAL PRIMARY KEY,
	timedate 	TIMESTAMP,
	vehicle_id 	TEXT,
	longitude 	DOUBLE PRECISION,
	latitude 	DOUBLE PRECISION,
	instant_speed 	REAL,
	heading 	REAL,
	trainset_id 	TEXT,
	predictable 	BOOLEAN,
	average_speed 	REAL,
	over_minspeed 	BOOLEAN,
	on_run 		BOOLEAN,
	run_block 	INTEGER,
	geom 		GEOMETRY(POINT, 26943)
	);
