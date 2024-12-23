SELECT * FROM public.dimessages
where messagetype = 'Validation' or messagetype = 'PCR';

SELECT
(SELECT messagedateandtime FROM public.dimessages
where messagetype = 'PCR' and batchid = 1)
-
(SELECT messagedateandtime FROM public.dimessages
where messagetype = 'PCR' and batchid = 0)
as elapsed_time;

