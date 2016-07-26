SELECT * FROM gotov 
 WHERE rowid in (SELECT MIN(rowid) MN 
                    FROM gotov 
                   GROUP BY templ_text, db_name
                 )