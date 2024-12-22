truncate table hr;

DO $$ 
DECLARE
    base_path TEXT;
BEGIN
    -- Retrieve the base path from the config table
    SELECT value_text INTO base_path
    FROM config
    WHERE key_name = 'base_path';

    -- Use dynamic SQL to execute the COPY command with the file path
    EXECUTE format('COPY hr FROM %L DELIMITER '','' CSV;', base_path || '/data/Batch1/HR.csv');

END $$;


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