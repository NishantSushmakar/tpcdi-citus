truncate table finwire_fin;

DO $$ 
DECLARE
    base_path TEXT;
BEGIN
    -- Retrieve the base path from the config table
    SELECT value_text INTO base_path
    FROM config
    WHERE key_name = 'base_path';

    -- Use dynamic SQL to execute the COPY command with the file path
    EXECUTE format('COPY finwire_fin FROM %L DELIMITER '','' CSV;', base_path || '/finwire_fin.csv');

END $$;


truncate table financial;
insert into financial 
	select 
		cmp.sk_companyid as sk_companyid,
		year::numeric(4) as fi_year,
		quarter::numeric(1) as fi_qtr,
		qtrstartdate::date as fi_qtr_start_date,
		revenue::numeric(15,2) as fi_revenue,
		earnings::numeric(15,2) as fi_net_earn,
		eps::numeric(10,2) as fi_basic_eps,
		dilutedeps::numeric(10,2) as fi_dilut_eps,
		margin::numeric(10,2) as fi_margin,
		inventory::numeric(15,2) as fi_inventory,
		assets::numeric(15,2) as fi_assets,
		liability::numeric(15,2) as fi_liability,
		shout::numeric(12) as fi_out_basic,
		dilutedshout::numeric(12) as fi_out_dilut
	from finwire_fin fin_fin, dimcompany cmp
	where ((fin_fin.conameorcik = cmp.name) or (fin_fin.conameorcik = cmp.companyid::varchar))
	and left(fin_fin.pts, 8)::date >= cmp.effectivedate
	and left(fin_fin.pts, 8)::date < cmp.enddate;