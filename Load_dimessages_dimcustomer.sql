insert into dimessages
	select
	  now()
	, 1
	, 'DimCustomer'
	, 'Invalid customer tier'
	, 'Alert'
	, 'C_ID = ' || customerid || ', C_TIER = ' || tier
	from dimcustomer
	where tier not between 1 and 3;

insert into dimessages
	select
	  now()
	, 1
	, 'DimCustomer'
	, 'DOB out of range'
	, 'Alert'
	, 'C_ID = ' || customerid || ', C_DOB = ' || dob
	from dimcustomer
	where dob < (select * from batchdate) - interval '100 years'
	or dob > (select * from batchdate);