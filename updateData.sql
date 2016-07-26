declare 
  ssql varchar2(1000);
begin
  for i in (select * from brak_306_2) loop
    ssql := 'update document set cycle_num = 1 where load = ' || to_number(i.nom_por)
            || ' and otprnum between ' || to_number(i.start_ind) || ' and ' || to_number(i.end_ind);
    execute immediate ssql;        
  end loop;
end;
