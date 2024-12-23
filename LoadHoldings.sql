truncate table holdinghistory;

DO $$ 
DECLARE
    base_path TEXT;
BEGIN
    -- Retrieve the base path from the config table
    SELECT value_text INTO base_path
    FROM config
    WHERE key_name = 'base_path';

    -- Use dynamic SQL to execute the COPY command with the file path
    EXECUTE format('COPY holdinghistory FROM %L DELIMITER ''|'';', base_path || '/data/Batch1/holdinghistory.txt');

END $$;

truncate table factholdings;
insert into factholdings 
	select
	hh.hh_h_t_id as tradeid,
	hh.hh_t_id as currenttradeid,
	t.sk_customerid as sk_customerid,
	t.sk_accountid as sk_accountid,
	t.sk_securityid as sk_securityid,
	t.sk_companyid as sk_companyid,
	t.sk_closedateid as sk_dateid,
	t.sk_closetimeid as sk_timeid,
	t.tradeprice as currentprice,
	hh.hh_after_qty as currentholding,
	1 as batchid
	from holdinghistory hh, dimtrade t
	where hh.hh_t_id = t.tradeid;