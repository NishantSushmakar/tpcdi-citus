truncate table hr;
COPY hr FROM '/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/data/Batch1/HR.csv' delimiter ',' CSV;

truncate table dimbroker;
insert into dimbroker
	select 
	row_number() over(order by employeeid) as sk_brokerid,
	employeeid as brokerid,
	managerid,
	employeefirstname as firstname,
	employeelastname as lastname,
	employeemi as middleinitial,
	employeebranch as branch,
	employeeoffice as office,
	employeephone as phone,
	true as iscurrent,
	1 as batchid,
	(select min(datevalue) FROM dimdate) as effectivedate,
	'9999-12-31'::date as enddate
	from hr
	where employeejobcode = 314;