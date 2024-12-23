create index if not exists idx_tradehistory_t_id_dts on tradehistory(th_t_id, th_dts);
create index if not exists idx_trade_composite ON trade(t_id, t_ca_id, t_s_symb);
TRUNCATE TABLE dimtrade;

INSERT INTO dimtrade (
    tradeid,
    sk_brokerid,
    sk_createdateid,
    sk_createtimeid,
    sk_closedateid,
    sk_closetimeid,
    status,
    type,
    cashflag,
    sk_securityid,
    sk_companyid,
    quantity,
    bidprice,
    sk_customerid,
    sk_accountid,
    executedby,
    tradeprice,
    fee,
    commission,
    tax,
    batchid
)
WITH first_trade_history AS (
    -- Get the first occurrence of each trade to establish create dates
    SELECT 
        t_id,
        th_dts,
        th_st_id,
        t_tt_id,
        ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY th_dts) as rn
    FROM 
        trade t
        JOIN tradehistory th ON t.t_id = th.th_t_id
),
last_trade_history AS (
    -- Get the last occurrence of each trade to establish close dates
    SELECT 
        t_id,
        th_dts,
        th_st_id,
        ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY th_dts DESC) as rn
    FROM 
        trade t
        JOIN tradehistory th ON t.t_id = th.th_t_id
),
trade_dates AS (
    -- Calculate create and close dates based on requirements
    SELECT 
        t.t_id,
        -- Create dates based on first occurrence meeting conditions
        CASE
            WHEN (fth.th_st_id = 'SBMT' AND fth.t_tt_id IN ('TMB', 'TMS')) 
              OR fth.th_st_id = 'PNDG'
            THEN TO_CHAR(DATE_TRUNC('second', fth.th_dts)::date, 'YYYYMMDD')::numeric
        END as sk_createdateid,
        CASE
            WHEN (fth.th_st_id = 'SBMT' AND fth.t_tt_id IN ('TMB', 'TMS')) 
              OR fth.th_st_id = 'PNDG'
            THEN TO_CHAR(DATE_TRUNC('second', fth.th_dts)::time, 'HH24MISS')::numeric
        END as sk_createtimeid,
        -- Close dates based on last occurrence meeting conditions
        CASE
            WHEN lth.th_st_id IN ('CMPT', 'CNCL')
            THEN TO_CHAR(DATE_TRUNC('second', lth.th_dts)::date, 'YYYYMMDD')::numeric
        END as sk_closedateid,
        CASE
            WHEN lth.th_st_id IN ('CMPT', 'CNCL')
            THEN TO_CHAR(DATE_TRUNC('second', lth.th_dts)::time, 'HH24MISS')::numeric
        END as sk_closetimeid,
        fth.th_dts as first_trade_dts
    FROM 
        trade t
        JOIN first_trade_history fth ON t.t_id = fth.t_id AND fth.rn = 1
        JOIN last_trade_history lth ON t.t_id = lth.t_id AND lth.rn = 1
)
SELECT 
    t.t_id as tradeid,
    a.sk_brokerid,
    td.sk_createdateid,
    td.sk_createtimeid,
    td.sk_closedateid,
    td.sk_closetimeid,
    st.st_name as status,
    tt.tt_name as type,
    t.t_is_cash::boolean as cashflag,
    s.sk_securityid,
    s.sk_companyid,
    t.t_qty as quantity,
    t.t_bid_price as bidprice,
    a.sk_customerid,
    a.sk_accountid,
    t.t_exec_name as executedby,
    t.t_trade_price as tradeprice,
    t.t_chrg as fee,
    t.t_comm as commission,
    t.t_tax as tax,
    1 as batchid
FROM 
    trade t
    JOIN trade_dates td ON t.t_id = td.t_id
    -- Join with reference tables
    JOIN statustype st ON t.t_st_id = st.st_id
    JOIN tradetype tt ON t.t_tt_id = tt.tt_id
    -- Join with DimSecurity using effective dating
    JOIN dimsecurity s ON t.t_s_symb = s.symbol
        AND td.first_trade_dts::date >= s.effectivedate
        AND td.first_trade_dts::date < s.enddate
    -- Join with DimAccount using effective dating
    JOIN dimaccount a ON t.t_ca_id = a.accountid
        AND td.first_trade_dts::date >= a.effectivedate
        AND td.first_trade_dts::date < a.enddate;