select t2.period_id
     , t2.period_name
     , t2.manager
     , t2.zone_name 
     , sum(t2.good) as sm_good
     , count(*) as cnt
     , round(sum(t2.good) / count(*) * 100) as per
  from (select s.period_id
             , s.period_name
             , s.manager
             , s.zone_name     
             , case s.log_action
                 when 'сдюкем' then 0
                 else 
                   case s.stack_status_id
                     when 1 then 1
                     else 0
                   end
               end as good
          from (select st.stack_id, max(st.log_id) as mx_log_id
                  from log_stack_vw st
                 group by st.stack_id
               ) t
          join log_stack_vw s on s.log_id = t.mx_log_id
        ) t2
 group by t2.period_id, t2.period_name, t2.manager, t2.zone_name
 order by t2.period_id, t2.period_name, t2.manager, t2.zone_name
