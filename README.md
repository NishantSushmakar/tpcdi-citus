# tpcdi-citus
This Project is implemented as a part of INFO-H-419: Data Warehouses course at ULB, supervised by  Prof. Esteban Zimányi. 

Implemented by: Sara Saad, Marwah Sulaiman, Nishant Sushmakar, and Olha Baliasina.

The project aims to perform the [TPC-DI benchmark](https://www.tpc.org/tpcdi/default5.asp) on Citus Database (a Postgres distributed extension) locally installed on MacOS machine.

- Runtime of ETL process on all selected scales are reported [here](Results). The csv file includes individual time for each step of the ETL process as well as the results of validation queries on each scale. 


In order to replicate our steps, the following should be done:


1. Configure Java in your mac using the section 5.3 given in the report.

2. Clone this repository, cd to the [Tools](Tools) directory in terminal, and execute this command to generate data:

               java -jar DIGen.jar -o ../data/ -sf 5

               notes: 
          		  - you can use your preferred destination directory instead of data folder and make sure it exists before executing the command.
          		  - here we are generating 500 MB data, same command will be used to generate bigger scales with only changing the scale and dir options.

3. Setup Airflow locally on the machine by following section 5.6 in the report.
4. Install Citus locally on the machine by following section 5.7 Citus Cluster Setup in the report. 
5. Add the DAG under the DAGs to you airflow dag directory
6. Change the path in the DAG to the project path in your PC

5. Restart Airflow and run the DAG from the UI
