select count(*) 
  from (select a, b, count(*) as cnt from t1 group by a, b) t1 
  full OUTER JOIN (select a, b, count(*) as cnt from t2 group by a, b) t2 on t1.a = t2.a and t1.b = t2.b and t1.cnt = t2.cnt
 where t1.cnt is null or t2.cnt is null;

delete from t
 where exists (select rowid
                 from (select rowid
                            , row_number() over (partition by a, b order by a) as rn
                         from t
                      ) t1
                where rn > 1
                  and t.rowid = t1.rowid
              );

select curr_id, rate
  from (select curr_id
             , date_rate as date_begin
             , lead(date_rate, 1, DATE_RATE) over (partition by CURR_ID order by CURR_ID, DATE_RATE) - 1 as date_finish
             , rate 
          from rates
        )
 where (curr_id = 1 and to_date('03.01.2010', 'dd.mm.yyyy') between date_begin and date_finish)
    or (curr_id = 2 and to_date('10.01.2010', 'dd.mm.yyyy') between date_begin and date_finish);
    
select dt_begin
     , dt_end
     , dt_end - dt_begin + 1 as days
     , rest
     , prc
     , round(rest * prc / 100 * (dt_end - dt_begin + 1) / 365, 2) as percent_sum
  from (select ACC
             , DT
             , REST
             , PRC
             , lg
             , ld
             , decode(lg, 0, lag(dt) over (order by dt), dt) as dt_begin
             , decode(ld, 1, dt, null) as dt_end
          from(select ACC
                    , DT
                    , REST
                    , PRC 
                    , decode(lag(rest || '_' || PRC) over (order by DT), rest || '_' || PRC, 0, 1) as lg
                    , decode(lead(rest || '_' || PRC) over (order by DT), rest || '_' || PRC, 0, 1) as ld
                 from T_PRC
              )
         where lg = 1 or ld = 1
      )
 where dt_end is not null;