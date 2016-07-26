create or replace view vw_month_used_state as
select 'ÇÀÎ "ðÎÃÀ È êÎÏÛÒÀ"' as name
     , t2.idx || ' ' || t2.month || ' ¹ ' || t2.bgn || ' - ' || t2.finish as shpi
     , t2.mn || ' - ' || t2.mx as rn
from
  (
      select t1.idx
           , t1.month
           , t1.bgn
           , t1.finish
           , t1.mn
           , t1.mx as mx_old
           --, lead(t1.mx, 1, null) over (order by idx, month, bgn, finish) mx
           , lead(t1.mx, 1, null) over (order by idx, bgn, d_b) mx
      from
      (
       select t0.* from
         (
            select r.idx
                 , r.month
                 , r.bgn
                 , r.finish
                 , d.bgn d_b
                 , d.finish d_f
                 --, d.bgn - lag(d.finish, 1, d.bgn - 1) over (partition by r.idx, r.bgn order by r.idx, r.bgn, d.bgn) lg
                 --, lead(d.bgn, 1, d.finish + 1) over (partition by r.idx, r.bgn order by r.idx, r.bgn, d.bgn) - d.finish ld
                 , decode(d.bgn - lag(d.finish, 1, d.bgn) over (partition by r.idx, r.bgn order by r.idx, r.bgn, d.bgn), 1, 0, d.bgn) mn
                 , decode(lead(d.bgn, 1, d.finish) over (partition by r.idx, r.bgn order by r.idx, r.bgn, d.bgn) - d.finish, 1, 0, d.finish) mx
            from rpo_data_rf_vw r
            join distribution d on d.rpo_data_id = r.id
            join work w on d.work_id = w.id
            where w.state = 0
              and r.month = distribution_define.PostMonth(sysdate)--68
         ) t0
      where t0.mn != 0
        or t0.mx != 0
      ) t1
     -- order by idx, bgn, d_b
  ) t2
where ((mx > 0) and (mn > 0));
