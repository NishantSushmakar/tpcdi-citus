truncate table finwire_sec;

DO $$
DECLARE
    base_path TEXT;
BEGIN
    -- Retrieve the base path from the config table
    SELECT value_text INTO base_path
    FROM config
    WHERE key_name = 'base_path';

    -- Use dynamic SQL to execute the COPY command with the file path
    EXECUTE format('COPY finwire_sec FROM %L DELIMITER '','' CSV;', base_path || '/finwire_sec.csv');

END $$;



truncate table dimsecurity;
insert into dimsecurity
	SELECT
		row_number() over(ORDER BY symbol, fin_sec.pts) as sk_securityid,
        symbol,
        issuetype       AS issue,
        st.st_name      AS status,
        fin_sec.name,
        exid            AS exchangeid,
        c.sk_companyid  AS sk_companyid,

        shout::numeric(12,0)           AS sharesoutstanding,
        LEFT(firsttradedate, 8)::date   AS firsttrade,
        LEFT(firsttradeexchg, 8)::date  AS firsttradeonexchange,
        dividend::numeric(10,2)         AS dividend,

        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fin_sec.pts DESC) = 1
            THEN TRUE
            ELSE FALSE
        END AS iscurrent,
        1 AS batchid,
        LEFT(fin_sec.pts, 8)::date AS effectivedate,
        '9999-12-31'::date         AS enddate

    FROM finwire_sec fin_sec
    JOIN statustype st
        ON fin_sec.status = st.st_id
    JOIN dimcompany c
        ON (
            (fin_sec.conameorcik ~ '^[0-9]+$' AND c.companyid = fin_sec.conameorcik::numeric)
            OR
            (fin_sec.conameorcik !~ '^[0-9]+$' AND c.name = fin_sec.conameorcik)
        )
        AND LEFT(fin_sec.pts,8)::date >= c.effectivedate
        AND LEFT(fin_sec.pts,8)::date < c.enddate
    ORDER BY symbol, fin_sec.pts;

