truncate table finwire_cmp;

DO $$ 
DECLARE
    base_path TEXT;
BEGIN
    -- Retrieve the base path from the config table
    SELECT value_text INTO base_path
    FROM config
    WHERE key_name = 'base_path';

    -- Use dynamic SQL to execute the COPY command with the file path
    EXECUTE format('COPY finwire_cmp FROM %L DELIMITER '','' CSV;', base_path || '/finwire_cmp.csv');

END $$;



-- dimcompany
truncate table dimcompany;
insert into dimcompany
	select 
	row_number() over(order by cik) as sk_companyid,
	cik::numeric(11) as companyid, 
	st.st_name as status,
	companyname as name, 
	ind.in_name as industry,
	CASE 
        WHEN fin_cmp.sprating not in ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D') 
		THEN null
		ELSE fin_cmp.sprating
    END as sprating, 
	CASE 
        WHEN fin_cmp.sprating not in ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D')
		THEN null
		WHEN fin_cmp.sprating like 'A%' or fin_cmp.sprating like 'BBB%' 
		THEN false
		ELSE true 
    END as islowgrade,
	ceoname as ceo,
	addrline1 as addressline1,
	addrline2 as addressline2,
	postalcode, 
	city, 
	stateprovince as stateprov,
	country, 
	description, 
	foundingdate::date,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY cik ORDER BY pts DESC) = 1 
        THEN TRUE 
        ELSE FALSE 
    END AS iscurrent,
	1 as batchid,
	left(fin_cmp.pts, 8)::date as effectivedate,
	'9999-12-31'::date as enddate 
	from 
		finwire_cmp fin_cmp left join statustype st on fin_cmp.status = st.st_id
        left join industry ind on fin_cmp.industryid = ind.in_id; 

-- Alerts for dimcompany
truncate table dimessages;
insert into dimessages
	select 
	now(),
	1 as batchid,
	'DimCompany' as messagesource,
	'Invalid SPRating' as messagetext,
	'Alert' as messagetype,
	('CO_ID = ' || cik::varchar || ', CO_SP_RATE = ' || sprating::varchar) as messagedata
	from finwire_cmp
	where sprating not in ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D');
