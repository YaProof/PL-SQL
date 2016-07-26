DELETE from brak_who t
where t.rowid in (select rowid from 
                    (select t.rowid, 
                            t.phone, 
                            row_number() over (partition by t.phone order by t.phone) rn
                       from brak_who t)
                 where rn > 1)
