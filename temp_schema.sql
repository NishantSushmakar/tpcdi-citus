drop table if exists hr;
create table hr(
	employeeid numeric(11) not null check(employeeid >= 0),
	managerid numeric(11) not null check(managerid >= 0),
	employeefirstname char(30) not null,
	employeelastname char(30) not null,
	employeemi char(1),
	employeejobcode numeric(3) check(employeejobcode >= 0),
	employeebranch char(30),
	employeeoffice char(10),
	employeephone char(14)	
);

drop table if exists batchdate;
create table batchdate(
	batchdate date not null	
);

drop table if exists finwire_cmp;
create table finwire_cmp(
	pts char(15) check(length(pts) > 0),
	rectype char(3) check(length(rectype) > 0),
	companyname char(60) check(length(companyname) > 0),
	cik char(10) check(length(cik) > 0),
	status char(4) check(length(status) > 0),
	industryid char(2) check(length(industryid) > 0),
	sprating char(4) check(length(sprating) > 0),
	foundingdate char(8),
	addrline1 char(80) check(length(addrline1) > 0),
	addrline2 char(80),
	postalcode char(12) check(length(postalcode) > 0),
	city char(25) check(length(city) > 0),
	stateprovince char(20) check(length(stateprovince) > 0),
	country char(24),
	ceoname char(46) check(length(ceoname) > 0),
	description char(150) check(length(description) > 0)
);

drop table if exists finwire_sec;
create table finwire_sec(
	pts char(15) check(length(pts) > 0),
	rectype char(3) check(length(rectype) > 0),
	symbol char(15) check(length(symbol) > 0),
	issuetype char(6) check(length(issuetype) > 0),
	status char(4) check(length(status) > 0),
	name char(70) check(length(name) > 0),
	exid char(6) check(length(exid) > 0),
	shout char(13) check(length(shout) > 0),
	firsttradedate char(8) check(length(firsttradedate) > 0),
	firsttradeexchg char(8) check(length(firsttradeexchg) > 0),
	dividend char(12) check(length(dividend) > 0),
	conameorcik char(60) check(length(conameorcik) > 0)
);

drop table if exists finwire_fin;
create table finwire_fin(
	pts char(15) check(length(pts) > 0),
	rectype char(3) check(length(rectype) > 0),
	year char(4) check(length(year) > 0),
	quarter char(1) check(length(quarter) > 0),
	qtrstartdate char(8) check(length(qtrstartdate) > 0),
	postingdate char(8) check(length(postingdate) > 0),
	revenue char(17) check(length(revenue) > 0),
	earnings char(17) check(length(earnings) > 0),
	eps char(12) check(length(eps) > 0),
	dilutedeps char(12) check(length(dilutedeps) > 0),
	margin char(12) check(length(margin) > 0),
	inventory char(17) check(length(inventory) > 0),
	assets char(17) check(length(assets) > 0),
	liability char(17) check(length(liability) > 0),
	shout char(13) check(length(shout) > 0),
	dilutedshout char(13) check(length(dilutedshout) > 0),
	conameorcik char(60) check(length(conameorcik) > 0)
);
