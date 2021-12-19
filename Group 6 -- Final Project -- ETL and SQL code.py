$ git clone https://github.com/hohiyan/APAN5310_GROUP6



#Group 6
#Yohan Ting, Yuhan Ye, Stephanie Wolfe
#APAN 5310
#Final Project
#December 19, 2021


#Import necessary packages
#import numpy as np
import numpy as np

#import pandas as pd
#from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import create_engine

import psycopg2


# create 3NF tables based on databse connections
# Pass the connection string to a variable, conn_url
conn_url = 'postgresql://postgres:123@localhost/Project'

# Create an engine that connects to PostgreSQL server
engine = create_engine(conn_url)

# Establish a connection
conn = engine.connect()

# dataset inspection and ETL process
main_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/DOMESTIC RAW DATASET.csv')
airport_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/L_AIRPORT.csv')
market_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/Market_population_income.csv')
aircraft_type_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/L_AIRCRAFT_TYPE.csv')
aircraft_group_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/L_AIRCRAFT_GROUP.csv')
aircraft_config_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/L_AIRCRAFT_CONFIG.csv')
service_class_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/L_SERVICE_CLASS.csv')
unique_carrier_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/L_UNIQUE_CARRIERS.csv')
carrier_group_new_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/L_CARRIER_GROUP_NEW.csv')
carrier_entity_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/L_UNIQUE_CARRIER_ENTITIES.csv')
carrier_region_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/L_REGION.csv')
distance_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/L_DISTANCE_GROUP_500.csv')
state_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/L_STATE_ABR_AVIATION.csv')
carrier_comp_factors_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/carrier_competition_factors.csv')
carrier_df = pd.read_csv('C:/Users/wolfe/Documents/Applied Analytics/SQL/Final Project/ETL/CARRIER_DECODE.csv')


main_df.head(5)

main_df.info()

# Pass the SQL statements that create all tables
stmt = """
    CREATE TABLE market_population_income
    (Market_ID    varchar(5)
    Market_name   varchar (30) NOT NULL,
    population_2019    int NOT NULL,
    Population_growth   int NOT NULL,
    income_per_capita        int NOT NULL,
    PRIMARY KEY (Market_ID));
    
    CREATE TABLE airport
    (airport_code    varchar (10),
    airport_name   varchar (30) NOT NULL,
    city           varchar (30) NOT NULL,
    state          varchar (10) NOT NULL,
    market_id      varcar(5) NOT NULL,
    PRIMARY KEY    (airport_code),
    FOREIGN KEY    (market_id) references market_population_income);
    
    CREATE TABLE origin
    (airport_code  varchar (10),
    airport_name   varchar (30) NOT NULL,
    PRIMARY KEY (airport_code),
    FOREIGN KEY (airport_code) REFERENCES airport);
    
    CREATE TABLE destination
    (airport_code    varchar (10),
    airport_name   varchar (30) NOT NULL,
    PRIMARY KEY (airport_code),
    FOREIGN KEY (airport_code) REFERENCES airport);
    
    CREATE TABLE aircraft_type
    (aircraft_type    varchar(3), 
    at_description    varchar (50) NOT NULL,
    PRIMARY KEY (aircraft_type));
    
    CREATE TABLE aircraft_group
    (aircraft_group   char(1), 
    ag_description    varchar (50) NOT NULL,
    PRIMARY KEY (aircraft_group));
    
    CREATE TABLE aircraft_config
    (aircraft_config     char(1),
    ac_description       varchar (50) NOT NULL,
    PRIMARY KEY (aircraft_config));
    
    CREATE TABLE distance_group
    (distance_group        char(1),
     dg_description        varchar (30) NOT NULL,
     PRIMARY KEY (distance_group));
    
    CREATE TABLE service_class_description
    (service_class    char(1) NOT NULL,
    sc_description    varchar (50) NOT NULL,
    PRIMARY KEY (service_class));
   
    CREATE TABLE carrier_group_new
    (carrier_group_new_code   char(1) NOT NULL,
    cgcn_description          varchar (100) NOT NULL,
    PRIMARY KEY (carrier_group_new_code));
    
    CREATE TABLE carrier_region
    (carrier_region_code      char(1) NOT NULL,
    crc_description           varchar (10) NOT NULL,
    PRIMARY KEY (UNIQUE_CARRIER_ENTITY_CODE));
    
    CREATE TABLE carrier_entity
    (carrier_entity_code    varchar(5) NOT NULL,
    ucec_description        varchar (100) NOT NULL,
    PRIMARY KEY (carrier_entity_code));
    
    CREATE TABLE carrier
    (unique_carrier_code   varchar (5),
    unique_carrier_name    varchar (50) NOT NULL,
    carrier_group_new_code char(1) NOT NULL,
    carrier_region_code    char(1) NOT NULL,
    carrier_entity_code    varchar(5) NOT NULL,
    PRIMARY KEY (unique_carrier_code),
    FOREIGN KEY (carrier_group_new_code) REFERENCES carrier_group_new,
    FOREIGN KEY (carrier_region_code) REFERENCES carrier_region,
    FOREIGN KEY (carrier_entity_code) REFERENCES carrier_entity);
    
    CREATE TABLE carrier_competition_factors
    (unique_carrier_code    varchar(5),
    passengers_carried     int,
    fleet_size             int,
    number_of_destinations   int,
    PRIMARY KEY (unique_carrier_code),
    FOREIGN KEY (unique_carrier_code) REFERENCES carrier);
     
    
    CREATE TABLE flight
    (ORIGIN               varchar (10) NOT NULL,
    DEST                  varchar (10) NOT NULL,
    UNIQUE_CARRIER        varchar (5) NOT NULL,
    AIRCRAFT_TYPE         varchar(3) NOT NULL,
    AIRCRAFT_GROUP        char(1) NOT NULL,
    AIRCRAFT_CONFIG       char(1) NOT NULL,
    MONTH                 int NOT NULL,
    QUARTER               int NOT NULL,
    DEPARTURES_PERFORMED  int NOT NULL,
    DISTANCE              int NOT NULL,
    DISTANCE_GROUP        char(1) NOT NULL,
    CLASS                 char(1) NOT NULL,
    SEATS                 numeric(4,1) NOT NULL,
    PAYLOAD               int NOT NULL,
    PASSENGERS            numeric(4,1) NOT NULL,
    RAMP_TO_RAMP          int NOT NULL,
    AIR_TIME              int NOT NULL,
    FREIGHT               int NOT NULL,
    MAIL                  int NOT NULL,
    PRIMARY KEY (ORIGIN, DEST, UNIQUE_CARRIER, AIRCRAFT_TYPE, DEPARTURES_PERFORMED, MONTH, AIR_TIME, PAYLOAD, SEATS, FREIGHT, RAMP_TO_RAMP, PASSENGERS, CLASS),
    FOREIGN KEY (ORIGIN) REFERENCES origin (airport_code),
    FOREIGN KEY (DEST) REFERENCES destination (airport_code),
    FOREIGN KEY (DISTANCE_GROUP) REFERENCES distance_group_description (distance_group), 
    FOREIGN KEY (AIRCRAFT_TYPE) REFERENCES aircraft_type (aircraft_type),
    FOREIGN KEY (AIRCRAFT_GROUP) REFERENCES aircraft_group (aircraft_group),
    FOREIGN KEY (AIRCRAFT_CONFIG) REFERENCES aircraft_config (aircraft_config),
    FOREIGN KEY (CLASS) REFERENCES service_class_description (service_class),
    FOREIGN KEY (UNIQUE_CARRIER) references carrier (unique_carrier_code));
    
"""

# CREATE TABLE market_population_income
market_df[['Market_ID', 'Market_name', 'population_2019', 'Population_growth', 'Income_per_capita']]. \
    drop_duplicates().to_sql(name='market_population_income', con=engine, if_exists='append', index=False)
    
conn.execute('alter table market_population_income add primary key("Market_ID")')


# CREATE TABLE airport
airport_df = airport_df.rename(columns={"Code": "airport_code"})

temp_airport_df = airport_df.Description.str.split('\:').str[-1].str.strip()

temp_airport_df.head(5)
temp_airport_df.name = 'airport_name'
airport_df = airport_df.join(temp_airport_df)
del airport_df['Description']

origin_airport = main_df[['ORIGIN', 'ORIGIN_CITY_MARKET_ID', 'ORIGIN_STATE_ABR']]. \
    drop_duplicates()

origin_airport = origin_airport.rename(columns={'ORIGIN': 'airport_code', 'ORIGIN_CITY_MARKET_ID': 'market_id', 
                                                'ORIGIN_STATE_ABR': 'state'})

dest_airport = main_df[['DEST', 'DEST_CITY_MARKET_ID', 'DEST_STATE_ABR']]. \
    drop_duplicates()

dest_airport = dest_airport.rename(columns={'DEST': 'airport_code', 'DEST_CITY_MARKET_ID': 'market_id', 
                                                'DEST_STATE_ABR': 'state'})

all_airport = pd.concat([origin_airport, dest_airport]). \
    drop_duplicates()
    
all_airport.head(5)

len(all_airport[['airport_code']])

airport_df = airport_df.merge(all_airport[['airport_code', 'market_id', 'state']], 
                 left_on='airport_code', right_on='airport_code', how='right')

airport_df = airport_df.merge(market_df[['Market_ID', 'Market_name']], left_on='market_id', right_on='Market_ID', how='left')

del airport_df['Market_ID']

airport_df = airport_df.rename(columns={'Market_name': 'city'})

airport_df.head(5)

airport_df. \
    drop_duplicates().to_sql(name='airport', con=engine, if_exists='append', index=False)

conn.execute('alter table airport add primary key("airport_code")')
conn.execute('alter table airport add foreign key("market_id") references market_population_income ("Market_ID")')

# create table origin

temp_origin_df = main_df[['ORIGIN']]
origin_df = temp_origin_df.rename(columns={'ORIGIN': 'airport_code'}). \
    drop_duplicates()

origin_df.head(5)
airport_df.head(5)

origin = origin_df.merge(airport_df[['airport_code', 'airport_name']], left_on='airport_code', right_on='airport_code', how='left')

origin.head(5)

origin. \
    drop_duplicates().to_sql(name='origin', con=engine, if_exists='append', index=False)
    
conn.execute('alter table origin add primary key("airport_code")')
conn.execute('alter table origin add foreign key("airport_code") references airport ("airport_code")')


# create table destination
temp_dest_df = main_df[['DEST']]
dest_df = temp_dest_df.rename(columns={'DEST': 'airport_code'}). \
    drop_duplicates()

dest = dest_df.merge(airport_df[['airport_code', 'airport_name']], left_on='airport_code', right_on='airport_code', how='left')

dest.head(5)

dest. \
    drop_duplicates().to_sql(name='destination', con=engine, if_exists='append', index=False)

conn.execute('alter table destination add primary key("airport_code")')
conn.execute('alter table destination add foreign key("airport_code") references airport ("airport_code")')

# create table aircraft_type
aircraft_type_df = aircraft_type_df[['Code', 'Description']]

aircraft_type_df = aircraft_type_df.rename(columns={'Code': 'aircraft_type', 'Description': 'at_description'})

aircraft_type_df. \
    drop_duplicates().to_sql(name='aircraft_type', con=engine, if_exists='append', index=False)
    
conn.execute('alter table aircraft_type add primary key("aircraft_type")')

# create table aircraft_group    
aircraft_group_df = aircraft_group_df[['Code', 'Description']]

aircraft_group_df = aircraft_group_df.rename(columns={'Code': 'aircraft_group', 'Description': 'ag_description'})

aircraft_group_df. \
    drop_duplicates().to_sql(name='aircraft_group', con=engine, if_exists='append', index=False)
    
conn.execute('alter table aircraft_group add primary key("aircraft_group")')

# create table aircraft_config
aircraft_config = aircraft_config_df.rename(columns={'Code': 'aircraft_config', 'Description': 'ac_description'})
aircraft_config.head(5)
aircraft_config. \
    drop_duplicates().to_sql(name='aircraft_config', con=engine, if_exists='append', index=False)
    
conn.execute('alter table aircraft_config add primary key("aircraft_config")')

# create table distance_group_description
distance_group = distance_df.rename(columns={'Code': 'distance_group', 'Description': 'dg_description'})

distance_group. \
    drop_duplicates().to_sql(name='distance_group_description', con=engine, if_exists='append', index=False)
    
conn.execute('alter table distance_group_description add primary key("distance_group")')

# create table service_class_description
service_class = service_class_df.rename(columns={'Code': 'service_class', 'Description': 'sc_description'})
    
service_class. \
    drop_duplicates().to_sql(name='service_class_description', con=engine, if_exists='append', index=False)
    
conn.execute('alter table service_class_description add primary key("service_class")')

# create table carrier_group_new
carrier_group_new = carrier_group_new_df.rename(columns={'Code': 'carrier_group_new', 'Description': 'cgcn_description'})
carrier_group_new.head(5)

carrier_group_new. \
    drop_duplicates().to_sql(name='carrier_group_new', con=engine, if_exists='append', index=False)

conn.execute('alter table carrier_group_new add primary key("carrier_group_new")')

# create table carrier_region
carrier_region = carrier_region_df.rename(columns={'Code': 'carrier_region_code', 'Description': 'crc_description'})

carrier_region. \
    drop_duplicates().to_sql(name='carrier_region', con=engine, if_exists='append', index=False)
    
conn.execute('alter table carrier_region add primary key("carrier_region_code")')

# create table carrier_entity
carrier_entity = carrier_entity_df.rename(columns={'Code': 'unique_carrier_entity_code', 'Description': 'ucec_description'})

carrier_entity. \
    drop_duplicates().to_sql(name='carrier_entity', con=engine, if_exists='append', index=False)
    
conn.execute('alter table carrier_entity add primary key("unique_carrier_entity_code")')

# create carrier_competition_factors
carrier_competition_factors = carrier_comp_factors_df.dropna(subset=['unique_carrier_code'])

carrier_competition_factors. \
    drop_duplicates().to_sql(name='carrier_competition_factors', con=engine, if_exists='append', index=False)

conn.execute('alter table carrier_competition_factors add primary key("unique_carrier_code")')

# create table carrier
temp_carrier = carrier_df[['unique_carrier_code', 'unique_carrier_name', 'carrier_group_new', 'carrier_region_code', 'unique_carrier_entity_code']]

carrier = temp_carrier.drop_duplicates(subset='unique_carrier_code', keep='first')

carrier = carrier.dropna(subset=['unique_carrier_code'])
    
carrier. \
    drop_duplicates().to_sql(name='carrier', con=engine, if_exists='append', index=False)

conn.execute('alter table carrier add primary key("unique_carrier_code")')
conn.execute('alter table carrier add foreign key("unique_carrier_code") references carrier_competition_factors ("unique_carrier_code")')
conn.execute('alter table carrier add foreign key("carrier_group_new") references carrier_group_new ("carrier_group_new")')
conn.execute('alter table carrier add foreign key("carrier_region_code") references carrier_region ("carrier_region_code")')
conn.execute('alter table carrier add foreign key("unique_carrier_entity_code") references carrier_entity ("unique_carrier_entity_code")')


# create table flight
main_df['SEATS'] = main_df['SEATS'].astype(float)
main_df['PASSENGERS'] = main_df['PASSENGERS'].astype(float)
flight_df = main_df[['ORIGIN', 'DEST', 'UNIQUE_CARRIER', 'AIRCRAFT_TYPE', 
    'AIRCRAFT_GROUP', 'AIRCRAFT_CONFIG', 'MONTH', 'QUARTER', 'DEPARTURES_PERFORMED', 'DISTANCE', 'DISTANCE_GROUP',
    'CLASS', 'SEATS', 'PAYLOAD', 'PASSENGERS', 'RAMP_TO_RAMP', 'AIR_TIME', 'FREIGHT', 'MAIL']]

flight_df. \
    drop_duplicates().to_sql(name='flight', con=engine, if_exists='append', index=False)

conn.execute('alter table flight add primary key("ORIGIN", "DEST", "UNIQUE_CARRIER", "AIRCRAFT_TYPE", "DEPARTURES_PERFORMED", "MONTH", "AIR_TIME", "PAYLOAD", "SEATS", "FREIGHT", "RAMP_TO_RAMP", "PASSENGERS", "CLASS")')

conn.execute('alter table flight add foreign key("ORIGIN") references origin ("airport_code")')
conn.execute('alter table flight add foreign key("DEST") references destination ("airport_code")')
conn.execute('alter table flight add foreign key("UNIQUE_CARRIER") references carrier ("unique_carrier_code")')
conn.execute('alter table flight add foreign key("AIRCRAFT_TYPE") references aircraft_type ("aircraft_type")')
conn.execute('alter table flight add foreign key("AIRCRAFT_GROUP") references aircraft_group ("aircraft_group")')
conn.execute('alter table flight add foreign key("AIRCRAFT_CONFIG") references aircraft_config ("aircraft_config")')
conn.execute('alter table flight add foreign key("DISTANCE_GROUP") references distance_group_description ("distance_group")')
conn.execute('alter table flight add foreign key("CLASS") references service_class_description ("service_class")')

#Execute query 1
#What are the top 20 most popular but underserved passenger routes?
#What are their average daily passenger numbers and how many competitors are there?
stmt = """
SELECT *
FROM 
    (SELECT "ORIGIN", "DEST", SUM("PASSENGERS")/sum("SEATS") AS "total_load_factor",
     SUM("PASSENGERS")/30 as "daily_passengers", COUNT(DISTINCT("UNIQUE_CARRIER")) AS competitor_num
    FROM flight
    WHERE "AIRCRAFT_CONFIG" = 1 AND
		  			"SEATS" > 0
    GROUP BY "ORIGIN", "DEST"
    ) AS f
WHERE "total_load_factor" > 0.95
ORDER BY "daily_passengers" DESC, "total_load_factor" DESC
LIMIT(20);
    """

results = conn.execute(stmt).fetchall()
column_names = results[0].keys()
Q1 = pd.DataFrame(results, columns=column_names)
Q1

#Execute query 2
#What are the top 20 most popular but overserved passenger routes?
#What are their average daily passenger numbers and how many competitors are there?
stmt = """
SELECT *
FROM (
    SELECT "ORIGIN", "DEST", SUM("PASSENGERS")/sum("SEATS") AS "Total_Load_factor", SUM("PASSENGERS")/30 as "daily pax", COUNT(DISTINCT("UNIQUE_CARRIER")) AS competitor_num
    FROM flight
    WHERE "AIRCRAFT_CONFIG" = 1 AND "SEATS" > 1
    GROUP BY "ORIGIN", "DEST"
    ) AS f
WHERE "Total_Load_factor" < 0.5
ORDER BY "daily pax" DESC, "Total_Load_factor" DESC
LIMIT(20);
    """

results = conn.execute(stmt).fetchall()
column_names = results[0].keys()
Q2 = pd.DataFrame(results, columns=column_names)
Q2

#Execute query 3
#Which origin airports have the highest passengers/seats ratio? (I.e. This airport is underserved.)
stmt = """
SELECT *
FROM (
    SELECT "ORIGIN", SUM("PASSENGERS")/sum("SEATS") AS "total_load_factor", SUM("PASSENGERS")/30 as "daily_passengers", COUNT(DISTINCT("UNIQUE_CARRIER")) AS "competitor_num"
    FROM flight
    WHERE "AIRCRAFT_CONFIG" = 1 AND
			"SEATS" > 0
    GROUP BY "ORIGIN"
    ) AS f
WHERE "daily_passengers" > 5000 AND "total_load_factor" > 0.5
ORDER BY "total_load_factor" DESC, "daily_passengers" DESC
LIMIT (10);
    """

results = conn.execute(stmt).fetchall()
column_names = results[0].keys()
Q3 = pd.DataFrame(results, columns=column_names)
Q3

#Execute query 4
#Which origin city has the highest passenger/seats ratio?
#What is the daily passenger volume in this city?
#How many airlines are serving this airport?
stmt = """
WITH Y AS
	(WITH X AS
		(SELECT "ORIGIN", city AS origin_city, "UNIQUE_CARRIER", "PASSENGERS" AS passengers_per_month,
	 	"SEATS" AS seats_per_month
		FROM (flight JOIN origin ON flight."ORIGIN"=origin.airport_code) JOIN airport ON origin.airport_code = airport.airport_code
		WHERE "SEATS" > 0 AND
	 	"AIRCRAFT_CONFIG" = 1 AND
 		"AIRCRAFT_GROUP" != 3
		GROUP BY "ORIGIN", origin_city, "UNIQUE_CARRIER", "PASSENGERS", "SEATS"
		ORDER BY "ORIGIN")
	SELECT "ORIGIN", origin_city,  COUNT("UNIQUE_CARRIER") AS number_airlines,
 		SUM(passengers_per_month)/SUM("seats_per_month") AS origin_city_load_factor,
 		AVG(passengers_per_month)/30 AS daily_passengers,
 		DENSE_RANK () OVER (ORDER BY SUM(passengers_per_month)/SUM("seats_per_month") DESC) as load_factor_rank
	FROM X
	GROUP BY "ORIGIN", origin_city)
SELECT *
FROM Y
WHERE load_factor_rank = 1;
    """

results = conn.execute(stmt).fetchall()
column_names = results[0].keys()
Q4 = pd.DataFrame(results, columns=column_names)
Q4

#Execute query 5
#Based on the city selected in question 3, is there a dominant competitor in this airport?
stmt = """
SELECT  DISTINCT "UNIQUE_CARRIER", unique_carrier_name,
		SUM("PASSENGERS") AS "total_passengers",
		SUM("PASSENGERS")*100.0/SUM(SUM("PASSENGERS")) OVER () AS "MKT_SHARE"
FROM flight JOIN carrier ON (flight."UNIQUE_CARRIER"=carrier.unique_carrier_code)
WHERE "AIRCRAFT_CONFIG" = 1 AND
"ORIGIN" = 'HRL' AND
"DEPARTURES_PERFORMED" > 1
GROUP BY DISTINCT "UNIQUE_CARRIER", unique_carrier_name
ORDER BY "total_passengers" DESC;
    """

results = conn.execute(stmt).fetchall()
column_names = results[0].keys()
Q5 = pd.DataFrame(results, columns=column_names)
Q5

#Execute query 6
#What are the most underserved “state” pairs? For example, NY-CA, FL-TX
stmt = """
WITH A AS
    (SELECT "ORIGIN", "DEST", "UNIQUE_CARRIER", "MONTH", "DEPARTURES_PERFORMED", "SEATS", "PASSENGERS", state AS origin_state
     FROM (flight JOIN origin ON flight."ORIGIN"=origin.airport_code) JOIN airport ON origin.airport_code = airport.airport_code
     WHERE flight."AIRCRAFT_CONFIG" = 1),
B AS
(SELECT origin_state || '-' || state AS state_pair, "SEATS", "PASSENGERS"
FROM (A JOIN destination ON A."DEST"=destination.airport_code) JOIN
 	  airport ON destination.airport_code = airport.airport_code
WHERE "DEPARTURES_PERFORMED" > 0 AND
	  "SEATS" > 10)
SELECT state_pair, SUM("PASSENGERS")/SUM("SEATS") AS load_factor
FROM B
WHERE "SEATS" > 10
GROUP BY state_pair
ORDER BY load_factor DESC;
    """

results = conn.execute(stmt).fetchall()
column_names = results[0].keys()
Q6 = pd.DataFrame(results, columns=column_names)
Q6

#Execute query 7
#What is the longest air-time route in the US domestic market?
stmt = """
WITH X AS
(SELECT "ORIGIN" || '-' || "DEST" AS route, "ORIGIN", origin.airport_name AS origin_airport_name, 
         "DEST", destination.airport_name AS destination_airport_name, 
         "DISTANCE", "MONTH", "AIR_TIME", "DEPARTURES_PERFORMED"
FROM flight, origin, destination
WHERE flight."ORIGIN" = origin.airport_code AND
		flight."DEST" = destination.airport_code AND
		"DEPARTURES_PERFORMED" > 0 AND
		"AIRCRAFT_CONFIG" = 1 AND
 		"AIRCRAFT_GROUP" != 3
ORDER BY route),
Y AS
(SELECT route, origin_airport_name, destination_airport_name, "DISTANCE", SUM("AIR_TIME")/SUM("DEPARTURES_PERFORMED")/60 AS avg_route_airtime_hrs
FROM X
GROUP BY route, origin_airport_name, destination_airport_name, "DISTANCE"),
Z AS
(SELECT *, DENSE_RANK () OVER (ORDER BY avg_route_airtime_hrs DESC) as avg_air_time_rank
FROM Y
WHERE avg_route_airtime_hrs <14)
SELECT *
FROM Z
WHERE avg_air_time_rank = 1;
    """

results = conn.execute(stmt).fetchall()
column_names = results[0].keys()
Q7 = pd.DataFrame(results, columns=column_names)
Q7

#Execute query 8
#If this new airline’s current fleet has a range limit of 1999 miles, 
#what are the top 7 most underserved passenger routes in this range?
stmt = """
WITH X as
(SELECT origin.airport_code || '-' || destination.airport_code AS route,
 origin.airport_name || '/' || destination.airport_name AS route_name,
"SEATS", "PASSENGERS"
FROM flight, origin, destination
WHERE "DISTANCE" < 1999 AND
		"DEPARTURES_PERFORMED" > 0 AND
		"SEATS" > 0 AND
		"AIRCRAFT_CONFIG" = 1 AND
		"AIRCRAFT_GROUP" != 3 AND
		flight."ORIGIN" = origin.airport_code AND
		flight."DEST" = destination.airport_code)
SELECT route, route_name, SUM("PASSENGERS")/SUM("SEATS") AS load_factor,
DENSE_RANK () OVER (ORDER BY SUM("PASSENGERS")/SUM("SEATS") DESC) AS underserved_rank
FROM X
WHERE "SEATS" > 0
GROUP BY route, route_name
ORDER BY underserved_rank
LIMIT 7;
    """

results = conn.execute(stmt).fetchall()
column_names = results[0].keys()
Q8 = pd.DataFrame(results, columns=column_names)
Q8

#Execute query 9
#On average, how many departures are performed a day between the most populated city
#and the 2nd most populated city?
stmt = """
WITH biggest_market AS
	(WITH num_1 AS
		(SELECT *, DENSE_RANK() OVER (ORDER BY population_2019 DESC) AS rank
 		FROM airport
 		JOIN market_population_income ON market_population_income."Market_ID" = airport.market_id)
			SELECT *
			FROM num_1
			WHERE rank=1),
second_biggest_market AS
	(WITH num_2 AS
		(SELECT *, DENSE_RANK() OVER (ORDER BY population_2019 DESC) AS rank
 		FROM airport
 		JOIN market_population_income ON market_population_income."Market_ID" = airport.market_id)
			SELECT *
			FROM num_2			
		WHERE rank=2),
NY_LA AS
(SELECT SUM(flight."DEPARTURES_PERFORMED")/365 AS avg_NY_LA_flights_per_day
FROM (biggest_market JOIN flight ON biggest_market.airport_code = flight."ORIGIN") JOIN
	second_biggest_market ON (second_biggest_market.airport_code = flight."DEST")
WHERE	"DEPARTURES_PERFORMED" > 0 AND
		"SEATS" > 0 AND
		"AIRCRAFT_CONFIG" = 1 AND
 		"AIRCRAFT_GROUP" != 3),
LA_NY AS	
(SELECT SUM(flight."DEPARTURES_PERFORMED")/365 AS avg_LA_NY_flights_per_day
FROM (biggest_market JOIN flight ON biggest_market.airport_code = flight."DEST") JOIN
	second_biggest_market ON (second_biggest_market.airport_code = flight."ORIGIN")
WHERE	"DEPARTURES_PERFORMED" > 0 AND
		"SEATS" > 0 AND
		"AIRCRAFT_CONFIG" = 1 AND
 		"AIRCRAFT_GROUP" != 3)
				SELECT avg_NY_LA_flights_per_day, avg_LA_NY_flights_per_day
				FROM NY_LA, LA_NY;
    """

results = conn.execute(stmt).fetchall()
column_names = results[0].keys()
Q9= pd.DataFrame(results, columns=column_names)
Q9

#Execute query 10
#Which airline has the highest load factor and which one has the lowest?
#What are their fleet sizes?
stmt = """

WITH X AS
(SELECT "ORIGIN",
		"DEST",
		"MONTH",
		flight."UNIQUE_CARRIER",
		carrier.unique_carrier_name,
		carrier_competition_factors.fleet_size,
 		"SEATS",
 		"PASSENGERS"
FROM    (flight JOIN carrier ON flight."UNIQUE_CARRIER" = carrier.unique_carrier_code) NATURAL JOIN 
		carrier_competition_factors
WHERE	"DEPARTURES_PERFORMED" > 0 AND
		"SEATS" > 10 AND
		"AIRCRAFT_CONFIG" = 1 AND
 		"AIRCRAFT_GROUP" != 3),
Y AS
	(SELECT "UNIQUE_CARRIER", unique_carrier_name, fleet_size, SUM("PASSENGERS")/SUM("SEATS") AS avg_load_factor
	FROM X
	GROUP BY "UNIQUE_CARRIER",  unique_carrier_name, fleet_size),
Z AS
    (SELECT *, RANK () OVER (ORDER BY avg_load_factor DESC) AS avg_load_factor_rank
     FROM Y)
SELECT *
FROM Z
WHERE avg_load_factor_rank = 1 OR
				avg_load_factor_rank = 2 OR
				avg_load_factor_rank = 3 OR
				avg_load_factor_rank = 4 OR
				avg_load_factor_rank = 5 OR
				avg_load_factor_rank = 69 OR
				avg_load_factor_rank = 70 OR
				avg_load_factor_rank = 71 OR
				avg_load_factor_rank = 72 OR
				avg_load_factor_rank = 73;
    """

results = conn.execute(stmt).fetchall()
column_names = results[0].keys()
Q10= pd.DataFrame(results, columns=column_names)
Q10







