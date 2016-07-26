select lg.*
     , p.name
     , un.surname || ' ' || un.firstname || ' ' || un.middlename as boss
  from (select t.*
             , REGEXP_SUBSTR(t.msg, '[^#, #]+', 1, 1) as cnt
             , REGEXP_SUBSTR(t.msg, '[^#, #]+', 1, 2) as part
             , REGEXP_SUBSTR(t.msg, '[^#, #]+', 1, 3) as pzc
             , REGEXP_SUBSTR(t.msg, '[^#, #]+', 1, 4) as period
             , REGEXP_SUBSTR(t.msg, '[^#, #]+', 1, 5) as reklama
             , REGEXP_SUBSTR(t.msg, '[^#, #]+', 1, 6) as zone
             , REGEXP_SUBSTR(t.msg, '[^#, #]+', 1, 7) as stack_status
          from logging t
         where t.obj = 'нвепедэ' 
           and t.action = 'днаюбкем'
         order by t.id desc
        ) lg
  join period p on lg.period = p.id
  join user_name un on lg.usr = un.id
 where lg.pzc = 314
   and p.id = 8;
