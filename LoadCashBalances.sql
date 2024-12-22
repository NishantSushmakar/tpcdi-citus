truncate table cashtransaction;

DO $$ 
DECLARE
    base_path TEXT;
BEGIN
    -- Retrieve the base path from the config table
    SELECT value_text INTO base_path
    FROM config
    WHERE key_name = 'base_path';

    -- Use dynamic SQL to execute the COPY command with the file path
    EXECUTE format('COPY cashtransaction FROM %L DELIMITER ''|'';', base_path || '/data/Batch1/CashTransaction.txt');

END $$;

truncate table factcashbalances;
insert into factcashbalances
	with grouped as (
		select 
		a.sk_customerid as sk_customerid,
		a.sk_accountid as sk_accountid,
		d.sk_dateid as sk_dateid,
		sum(ct_amt) as ct_amt_day
		from cashtransaction ct, dimaccount a, dimdate d
		where ct.ct_ca_id = a.accountid
		and ct_dts::date >= a.effectivedate and ct_dts::date < a.enddate
		and ct_dts::date = d.datevalue
		group by a.sk_customerid, a.sk_accountid, d.sk_dateid
	)
    select
    sk_customerid,
    sk_accountid,
    sk_dateid,
    sum(ct_amt_day) over(partition by sk_accountid order by sk_dateid rows between unbounded preceding and current row) as cash,
    1 as batchid
    from grouped;
