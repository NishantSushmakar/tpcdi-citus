insert into dimessages
	select
	  now()
	, 1
	, 'FactMarketHistory'
	, 'No earnings for company'
	, 'Alert'
	, 'DM_S_SYMB = ' || s.symbol
	from factmarkethistory fmh
	inner join dimsecurity s
		on fmh.sk_securityid = s.sk_securityid
	where fmh.peratio is null
	or fmh.peratio = 0;