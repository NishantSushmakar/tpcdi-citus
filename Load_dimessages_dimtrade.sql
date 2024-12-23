insert into dimessages
	select
	  now()
	, 1
	, 'DimTrade'
	, 'Invalid trade commission'
	, 'Alert'
	, 'T_ID = ' || tradeid || ', T_COMM = ' || commission
	from dimtrade
	where commission is not null
	and commission > (tradeprice * quantity);
	
insert into dimessages
	select
	  now()
	, 1
	, 'DimTrade'
	, 'Invalid trade fee'
	, 'Alert'
	, 'T_ID = ' || tradeid || ', T_CHRG = ' || fee
	from dimtrade
	where fee is not null
	and fee > (tradeprice * quantity);