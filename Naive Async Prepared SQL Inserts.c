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

#define NUM_CONCURRENT_REQUESTS 250

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

typedef struct Flight_ Flight;

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

CassError prepare_stmt(CassSession* session, const char* sql, const CassPrepared** prepared) {
	CassError rc = CASS_OK;
	CassFuture* future = NULL;
	CassString query = cass_string_init(sql);

	future = cass_session_prepare(session, query);
	cass_future_wait(future);

	rc = cass_future_error_code(future);
	if(rc != CASS_OK) {
		print_error(future);
	} else {
		*prepared = cass_future_get_prepared(future);
  	}

  	cass_future_free(future);

	return rc;
}

void execute_prepared_stmt_async(CassSession* session, const CassPrepared * prepared, Flight* flight, int num_rows) {
	CassError rc = CASS_OK;
  	CassStatement* statement = NULL;
  	
	CassFuture* futures[NUM_CONCURRENT_REQUESTS];
	
	int i;
	for(i = 0; i < num_rows; ++i) {

		statement = cass_prepared_bind(prepared);

		cass_statement_bind_int32(statement, 0, flight[i].id);
		cass_statement_bind_int32(statement, 1, flight[i].year);
		cass_statement_bind_int32(statement, 2, flight[i].day_of_month);
		cass_statement_bind_string(statement, 3, cass_string_init(flight[i].fl_date));
		cass_statement_bind_int32(statement, 4, flight[i].airline_id);
		cass_statement_bind_string(statement, 5, cass_string_init(flight[i].carrier));
		cass_statement_bind_int32(statement, 6, flight[i].fl_num);
		cass_statement_bind_int32(statement, 7, flight[i].origin_airport_id);
		cass_statement_bind_string(statement, 8, cass_string_init(flight[i].origin));
		cass_statement_bind_string(statement, 9, cass_string_init(flight[i].origin_city_name));
		cass_statement_bind_string(statement, 10, cass_string_init(flight[i].origin_state_abr));
		cass_statement_bind_string(statement, 11, cass_string_init(flight[i].dest));
		cass_statement_bind_string(statement, 12, cass_string_init(flight[i].dest_city_name));
		cass_statement_bind_string(statement, 13, cass_string_init(flight[i].dest_state_abr));
		cass_statement_bind_int32(statement, 14, flight[i].dep_time);
		cass_statement_bind_int32(statement, 15, flight[i].arr_time);
		cass_statement_bind_int32(statement, 16, flight[i].actual_elapsed_time);
		cass_statement_bind_int32(statement, 17, flight[i].air_time);
		cass_statement_bind_int32(statement, 18, flight[i].distance);
		cass_statement_bind_int32(statement, 19, (flight[i].air_time/10));
		
		futures[i] = cass_session_execute(session, statement);

    	cass_statement_free(statement);
	}

  	
  	for(i = 0; i < num_rows; ++i) {
    	CassFuture* future = futures[i];

    	cass_future_wait(future);

    	rc = cass_future_error_code(future);
    	if(rc != CASS_OK) {
      		print_error(future);
   		 }

    	cass_future_free(future);
  }
}

int main() {
	time_t start, stop;

	FILE *fp = fopen("/Users/carybourgeois/flights_exercise/flights_from_pg.csv", "r") ; 
	
	CassError rc = CASS_OK;
	CassCluster* cluster = create_cluster();
	CassSession* session = NULL;
	CassFuture* close_future = NULL;
	const CassPrepared* prepared = NULL;

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
 	
 	if(prepare_stmt(session, "INSERT INTO flights \
 								(id, year, day_of_month, fl_date, \
 								airline_id, carrier, fl_num, origin_airport_id, \
 								origin, origin_city_name, origin_state_abr, dest, \
 								dest_city_name, dest_state_abr, dep_time, arr_time, \
 								actual_elapsed_time, air_time, distance, air_time_grp) \
 								VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", 
 								&prepared) != CASS_OK) { 
 		return -1;
 	}
 	
 	if ( fp != NULL ) {
 		int rows = 0;
 		int async_rows = 0;
 		
 		Flight flight[NUM_CONCURRENT_REQUESTS];
 		  
   		while(!feof(fp)) {
                  
   			fscanf(fp, "%d, %d, %d, %[^,], %d, %[^,], %d, %d, %[^,], %[^,], %[^,], %[^,], %[^,], %[^,], %d, %d, %d, %d, %d \n", 
      			&flight[async_rows].id, &flight[async_rows].year, &flight[async_rows].day_of_month, flight[async_rows].fl_date, 
      			&flight[async_rows].airline_id, flight[async_rows].carrier, &flight[async_rows].fl_num, &flight[async_rows].origin_airport_id,
      			flight[async_rows].origin, flight[async_rows].origin_city_name, flight[async_rows].origin_state_abr, flight[async_rows].dest,
      			flight[async_rows].dest_city_name, flight[async_rows].dest_state_abr, &flight[async_rows].dep_time, &flight[async_rows].arr_time,
      			&flight[async_rows].actual_elapsed_time, &flight[async_rows].air_time, &flight[async_rows].distance);
      		
      		rows++;
      		async_rows++;
      		if ( async_rows == NUM_CONCURRENT_REQUESTS) {
      			execute_prepared_stmt_async(session, prepared, flight, async_rows);
      			
  				async_rows = 0;
      		}
           
        	/* if (rows > 2478) break; */
                               
		}  /* EOF */
		
		if ( async_rows > 0) {
			execute_prepared_stmt_async(session, prepared, flight, async_rows);	
      	}
      	 
		printf("%d Records loaded.\n", rows);
   
	}  /* File exists */ 
	
	time(&stop);
 
    printf("%.f Seconds total load time.\n", difftime(stop, start));   
   
	close_future = cass_session_close(session);
  	cass_future_wait(close_future);
	cass_future_free(close_future);
	cass_cluster_free(cluster);
	
	fclose( fp ); 
	
	return 0;   
  
}
