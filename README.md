# tpcdi-citus
This Project is implemented as a part of INFO-H-419: Data Warehouses course at ULB, supervised by  Prof. Esteban Zimányi. 

Implemented by: Sara Saad, Marwah Sulaiman, Nishant Sushmakar, and Olha Baliasina.

The project aims to perform the [TPC-DI benchmark](https://www.tpc.org/tpcdi/default5.asp) on Citus Database (a Postgres distributed extension) locally installed on MacOS machine.

- Runtime of ETL process on all selected scales are reported [here](Results). The csv file includes individual time for each step of the ETL process as well as the results of validation queries on each scale. 


In order to replicate our steps, the following should be done:

To run,

1. Setup Citus:

2. Setup Airflow:

3. Add the DAG under the DAGs to you airflow dag directory

4. Change the path in the DAG to the project path in your PC

5. Restart Airflow and run the DAG from the UI
