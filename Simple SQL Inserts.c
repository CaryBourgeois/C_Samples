/*
  Copyright (c) 2014 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#include "cassandra.h"

struct Flight_ { 
	int		id;
	int		year;
	int		day_of_month;
	char	fl_date[11];
	int 	airline_id;
	char 	carrier[3];
	int 	fl_num;
	int 	origin_airport_id;
	char 	origin[4];
	char	origin_city_name[20];
	char	origin_state_abr[4];
	char	dest[4];
	char	dest_city_name[20];
	char	dest_state_abr[4];
	int		dep_time;
	int		arr_time;
	int		actual_elapsed_time;
	int		air_time;
	int		distance;
} ;        

struct Flight_ Flight;

void print_error(CassFuture* future) {
  CassString message = cass_future_error_message(future);
  fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
}


CassCluster* create_cluster() {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, "127.0.0.1");
  return cluster;
}

CassError connect_session(CassCluster* cluster, CassSession** output) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_cluster_connect(cluster);

  *output = NULL;

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  } else {
    *output = cass_future_get_session(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_stmt(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(cass_string_init(query), 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main() { 
	char sql[1024];
	time_t start, stop;

	FILE *fp = fopen("/Users/carybourgeois/flights_exercise/flights_from_pg.csv", "r") ; 
	
	CassError rc = CASS_OK;
	CassCluster* cluster = create_cluster();
	CassSession* session = NULL;
	CassFuture* close_future = NULL;

	rc = connect_session(cluster, &session);
	if(rc != CASS_OK) {
		return -1;
	}
	
	execute_stmt(session, 
					"CREATE KEYSPACE IF NOT EXISTS exercise WITH \
						replication = {'class': 'SimpleStrategy','replication_factor': '1'};");
						
	execute_stmt(session,
					"USE exercise;");
						
	execute_stmt(session,
					"DROP TABLE IF EXISTS flights;");
					
	execute_stmt(session,
					"CREATE TABLE flights ( \
						id int, year int, day_of_month int, fl_date varchar, \
						airline_id int, carrier varchar, fl_num int, origin_airport_id int, \
						origin varchar, origin_city_name varchar, origin_state_abr varchar, dest varchar, \
						dest_city_name varchar, dest_state_abr varchar, dep_time int, arr_time int, \
						actual_elapsed_time int, air_time int, distance int, air_time_grp int, \
						PRIMARY KEY (carrier, origin, air_time_grp, id));");

 	time(&start);
 	
 	if ( fp != NULL ) {
 		int i = 0;  
   		while(!feof(fp)) {           
        	i++;
                  
   			fscanf(fp, "%d, %d, %d, %[^,], %d, %[^,], %d, %d, %[^,], %[^,], %[^,], %[^,], %[^,], %[^,], %d, %d, %d, %d, %d \n", 
      			&Flight.id, &Flight.year, &Flight.day_of_month, Flight.fl_date, 
      			&Flight.airline_id, Flight.carrier, &Flight.fl_num, &Flight.origin_airport_id,
      			Flight.origin, Flight.origin_city_name, Flight.origin_state_abr, Flight.dest,
      			Flight.dest_city_name, Flight.dest_state_abr, &Flight.dep_time, &Flight.arr_time,
      			&Flight.actual_elapsed_time, &Flight.air_time, &Flight.distance);
      
    		sprintf(sql, "INSERT INTO flights (id, year, day_of_month, fl_date, airline_id, carrier, fl_num, origin_airport_id, origin, origin_city_name, origin_state_abr, dest, dest_city_name, dest_state_abr, dep_time, arr_time, actual_elapsed_time, air_time, distance, air_time_grp) VALUES (%d, %d, %d, \'%s\', %d, \'%s\', %d, %d, \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', %d, %d, %d, %d, %d, %d);\n", 
        		Flight.id, Flight.year, Flight.day_of_month, Flight.fl_date, 
      			Flight.airline_id, Flight.carrier, Flight.fl_num, Flight.origin_airport_id,
      			Flight.origin, Flight.origin_city_name, Flight.origin_state_abr, Flight.dest,
      			Flight.dest_city_name, Flight.dest_state_abr, Flight.dep_time, Flight.arr_time,
      			Flight.actual_elapsed_time, Flight.air_time, Flight.distance, Flight.air_time/10 );
      			
      		/* printf("%s", sql); */
      		execute_stmt(session, sql);
           
        	/* if (i > 999) break; */
                               
		}  /* EOF */ 
		printf("%d Records loaded.\n", i);
   
	}  /* File exists */ 
	
	time(&stop);
 
    printf("%.f Seconds total load time.\n", difftime(start, stop));   
   
	close_future = cass_session_close(session);
  	cass_future_wait(close_future);
	cass_future_free(close_future);
	cass_cluster_free(cluster);
	
	fclose( fp ); 
	
	return 0;   
  
}
