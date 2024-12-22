COPY prospect_temp FROM '/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/data/Batch1/Prospect.csv' delimiter ',' CSV;

truncate table prospect;
insert into prospect
	with date_record_id as (
		select
		dd.sk_dateid
		from dimdate dd
		inner join batchdate bd
			on dd.datevalue = bd.batchdate
	)

	select
	  p.agencyid
	, dri.sk_dateid
	, dri.sk_dateid
	, 1
	, false 
	, p.lastname
	, p.firstname
	, p.middleinitial
	, p.gender
	, p.addressline1
	, p.addressline2
	, p.postalcode
	, p.city
	, p.state
	, p.country
	, p.phone
	, p.income
	, p.numbercars
	, p.numberchildren
	, p.maritalstatus
	, p.age
	, p.creditrating
	, p.ownorrentflag
	, p.employer
	, p.numbercreditcards
	, p.networth
	, nullif(btrim(btrim(btrim(btrim(btrim(
	  case
		when p.networth > 1000000 or p.income > 200000
		then 'HighValue'
		else ''
	  end
	  || '+' ||
	  case
		when p.numberchildren > 3 or p.numbercreditcards > 5
		then 'Expenses'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.age > 45
		then 'Boomer'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.income < 50000 or p.creditrating < 600 or p.networth < 100000
		then 'MoneyAlert'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.numbercars > 3 or p.numbercreditcards > 7
		then 'Spender'
		else ''
	  end
	  , '+')
	  || '+' ||
	  case
		when p.age < 25 and p.networth > 1000000
		then 'Inherited'
		else ''
	  end
	  , '+'), '')
	from prospect_temp p
	cross join date_record_id dri;