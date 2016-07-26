create or replace package distribution_define is

type DistributionData is record(
  tId work.id%type,
  tIdx rpo_data_rf.idx%type,
  tMonth rpo_data_rf.month%type,
  tBgn distribution.bgn%type,
  tEnd distribution.finish%type,
  tError pls_integer
  );
  
type DZ_DistributionData is record(
  tId work.id%type,
  tOtprType rpo_data_dz.otpr_type%type,
  tBgn distribution.bgn%type,
  tEnd distribution.finish%type,
  tError pls_integer
  );

type DistribDataTable is table of DistributionData;
type DZ_DistribDataTable is table of DZ_DistributionData;

create or replace package body distribution_define is

procedure RollbackAfterDays(aDay pls_integer) is
v pls_integer;
begin
  for cur in (select d.id
                from work w
                join distribution d on w.id = d.work_id
               where w.state = 1
                 and trunc(sysdate - when) = aDay
              ) loop
    v := CommitReserve(cur.id, 1);
  end loop;
end;

--Поиск ИД по баркоду
function IdFromBarcode(aBarcode string) return distribution.id%type as
  vInd pls_integer;
  vMonth pls_integer;
  vNom pls_integer;
  vRes pls_integer;
begin
  vRes := 0;
  
  if (Length(aBarcode) < 13) then
    return vRes;
  end if;

  vInd   := SubStr(aBarcode, 1, 6);
  vMonth := SubStr(aBarcode, 7, 2);
  vNom   := SubStr(aBarcode, 9, 5);

  select d.id into vRes
    from rpo_data_rf_vw r
    join distribution d on r.id = d.rpo_data_id
   where r.idx = vInd
     and r.month = vMonth
     and vNom between d.bgn and d.finish;

  return vRes;
end;

function AddProject(aName project.name%type) return project.id%type as
  vCnt pls_integer;
  vId project.id%type;
begin
  vId := 0;
  select count(*) into vCnt
    from project
   where upper(name) = upper(aName);

  if ((LENGTH(aName) > 0) and (vCnt = 0)) then
    insert into project (id, name) values (project_sq.nextval, aName) returning id into vId;
  end if;

  return vId;
end AddProject;

function AddWork(
    aProjectId work.project_id%type
  , aCnt work.cnt%type
  , aDt work.dt%type
) return work.id%type is
  WorkId pls_integer;
begin
  insert into work (id, project_id, cnt, dt)
  values (work_sq.nextval, aProjectId, aCnt, aDt)
  returning id into WorkId;

  return WorkId;
end AddWork;

--Определяет и выделяет РПО из используемого активного набора диапазонов
function FirstLevel(
    aProjectId project.id%type
  , aDeliveryPointsId rpo_data.DELIVERYPOINTS_id%type
  , aOwner rpo_data.owner_id%type
  , aCnt work.cnt%type
  , aDt work.dt%type
) return pls_integer is
  vD pls_integer;
  vPM pls_integer;
  vFlag boolean := false;
  vRes pls_integer := 0;

  cursor cur is
    select r.id
         , r.DELIVERYPOINTS_id
         , r.idx
         , r.month
         , r.bgn
         , r.finish
         , d.bgn as d_bgn
         , d.finish as d_finish
      from rpo_data_rf_vw r
      join distribution d on r.id = d.rpo_data_id
     where r.active = 0
       and r.month = vPM
       and r.DELIVERYPOINTS_id = aDeliveryPointsId
       and r.owner_id = aOwner
     order by r.idx, r.bgn, d.bgn;

  vRecord cur%rowtype;
  vPrevRecord cur%rowtype;
begin
  vPM := PostMonth(aDt);

  vPrevRecord.Id := 0;
  open cur;
  loop
    fetch cur into vRecord;
    exit when vFlag or cur%notfound;

    --Если новый диапазон
    if (vPrevRecord.Id != vRecord.id) then
      if (vPrevRecord.Id = 0) then --Первая строка
        vD := vRecord.d_bgn - vRecord.bgn;
        if (vD >= aCnt) then
          vRes := AddWork(aProjectId, aCnt, aDt);
          AddRange(vRes, vRecord.id, vRecord.bgn, vRecord.bgn + aCnt - 1);
          vFlag := True;
        end if;
        /*
        if (vCnt = 1) then
          if (vRecord.finish - vRecord.d_finish >= aCnt) then
            vRes := AddWork(aProjectId, aCnt, aDt);
            AddRange(vRes, vRecord.id, vRecord.d_finish + 1, vRecord.d_finish + aCnt);
            vFlag := True;
          end if;
        end if;
        */
      else
        vD := (vPrevRecord.Finish - vPrevRecord.d_Finish) + (vRecord.d_Bgn - vRecord.Bgn);
        if (vD >= aCnt) then
          if (vPrevRecord.Finish - vPrevRecord.d_Finish >= aCnt) then
            vRes := AddWork(aProjectId, aCnt, aDt);
            AddRange(vRes, vPrevRecord.id, vPrevRecord.d_Finish + 1, vPrevRecord.d_Finish + aCnt);
            vFlag := True;
          else
            vRes := AddWork(aProjectId, aCnt, aDt);
            AddRange(vRes, vPrevRecord.id, vPrevRecord.d_Finish + 1, vPrevRecord.Finish);
            AddRange(vRes, vRecord.id, vRecord.bgn, vRecord.bgn + (aCnt - (vPrevRecord.Finish - vPrevRecord.d_Finish)) - 1);
            vFlag := True;
          end if;
        end if;
      end if;
    else --Продолжение диапазона
      if (vRecord.d_bgn - vPrevRecord.d_Finish - 1 >= aCnt) then
        vRes := AddWork(aProjectId, aCnt, aDt);
        --AddRange(vRes, vRecord.id, vPrevRecord.d_Finish + 1, vPrevRecord.d_Finish + aCnt - 1); --20130425
        AddRange(vRes, vRecord.id, vPrevRecord.d_Finish + 1, vPrevRecord.d_Finish + aCnt);
        vFlag := True;
      end if;
    end if;
    vPrevRecord := vRecord;
  end loop;
  if (not vFlag) then
    if (vRecord.finish - vRecord.d_finish >= aCnt) then
      vRes := AddWork(aProjectId, aCnt, aDt);
      AddRange(vRes, vRecord.id, vRecord.d_finish + 1, vRecord.d_finish + aCnt);
    end if;
  end if;
  close cur;

  commit; --Необходим, что бы избежать пересечения диапазонов

  return vRes;
end FirstLevel;

--Определяет и выделяет РПО из самого большого остатка активного набора диапазонов + берет новый диапазон
function SecondLevel(
    aProjectId project.id%type
  , aDeliveryPointsId rpo_data.DELIVERYPOINTS_id%type
  , aOwner rpo_data.owner_id%type
  , aCnt work.cnt%type
  , aDt work.dt%type
) return pls_integer is
  vPM pls_integer;
  vD pls_integer := 0;
  vDm pls_integer := 0;
  vRes pls_integer := 0;
  vCnt pls_integer := 0;
  vF pls_integer := 0;
  vFm pls_integer := 0;
  vB pls_integer := 0;
  vBm pls_integer := 0;
  vExit boolean := False;

  cursor cur is
    select r.id, r.finish, max(d.finish) as mx_finish
      from rpo_data_rf_vw r
      join distribution d on r.id = d.rpo_data_id
     where r.active = 0
       and r.month = vPM
       and r.DELIVERYPOINTS_id = aDeliveryPointsId
       and r.owner_id = aOwner
     group by r.id, r.finish;

  cursor EmptyRange is
    select t.* from (
      select r.id, r.idx, r.month, r.bgn, r.finish, d.bgn as d_bgn
        from rpo_data_rf_vw r
        left join distribution d on r.id = d.rpo_data_id
       where r.active = 0
         and r.month = vPM
         and r.DELIVERYPOINTS_id = aDeliveryPointsId
         and r.owner_id = aOwner
       order by r.id
      ) t
    where t.D_bgn is null;

  TYPE IdArray IS TABLE OF pls_integer
  INDEX BY BINARY_INTEGER;

  vCur cur%rowtype;
  vEmptyRange EmptyRange%rowtype;
  vArId IdArray;
  i pls_integer := 0;
begin
  vPM := PostMonth(aDt);

  --Поиск максимального остатка
  open cur;
  loop
    fetch cur into vCur;
    exit when cur%notfound;
    vF := vCur.Finish;
    vB := vCur.Mx_Finish + 1;
    vD := vF - vB + 1;

    if (vD > vCnt) then
      vCnt := vD;
      vDm := vD;
      vBm := vB;
      vFm := vF;
      i := 1;
      vArId(i) := vCur.Id;
    end if;
  end loop;
  close cur;

  --Ищем новый диапазон, что бы количество все помещалось
  open EmptyRange;
  loop
    fetch EmptyRange into vEmptyRange;
    exit when vExit or EmptyRange%notfound;
    if ((aCnt - vCnt) <= (vEmptyRange.finish - vEmptyRange.bgn + 1)) then
      i := i + 1;
      vArId(i) := vEmptyRange.Id;
      vExit := True;
    end if;
  end loop;
  close EmptyRange;

  --Собираем несколько новых диапазонов
  if (not vExit) then
    open EmptyRange;
    loop
      fetch EmptyRange into vEmptyRange;
      exit when vExit or EmptyRange%notfound;
      vCnt := vCnt + vEmptyRange.finish - vEmptyRange.bgn + 1;
      i := i + 1;
      vArId(i) := vEmptyRange.Id;
      if (aCnt <= vCnt) then
        vExit := True;
      end if;
    end loop;
    close EmptyRange;
  end if;

  --Необходимое количество набрано
  if (vExit) then
    vCnt := aCnt;
    vRes := AddWork(aProjectId, aCnt, aDT);
    for i in 1..vArId.count loop
      if ((i = 1) and (vDm > 0)) then
        AddRange(vRes, vArId(i), vBm, vFm);
        vCnt := vCnt - (vFm - vBm + 1);
      else
        select r.bgn, r.finish into vB, vF
          from rpo_data r
         where r.id = vArId(i);

        if (vF - vB + 1 <= vCnt) then
          AddRange(vRes, vArId(i), vB, vF);
          vCnt := vCnt - (vF - vB + 1);
        else
          AddRange(vRes, vArId(i), vB, vB + vCnt - 1);
        end if;
      end if;
    end loop;
  end if;

  commit;

  return vRes;
end SecondLevel;


function FillBarcode(aWorkId work.id%type) return pls_integer as
  vCnt pls_integer;
  vN pls_integer := 0;
  vKod string(13);
  vBarcode string(14);
  s1 pls_integer := 0;
  s2 pls_integer := 0;
  s3 pls_integer := 0;
  k pls_integer;
begin
  select count(*) into vCnt
  from dual
  where exists (select 1 
                from work 
                where id = aWorkId
               );
  
  if (vCnt = 0) then
    return const.cResultWork;
  end if;

  select count(*) into vCnt
  from dual
  where exists (select 1 
                  from barcode b
                 where b.work_id = aWorkId
               );
               
  if (vCnt > 0) then
    return const.cResultNoError;
  end if;
  
  for cur in (select w.id, r.idx, r.month, d.bgn, d.finish
                from work w
                join distribution d on w.id = d.work_id
                join rpo_data_rf_vw r on d.rpo_data_id = r.id
               where w.id = aWorkId
              )
  loop
    for i in cur.bgn..cur.finish loop
      vN := vN + 1;
      vKod := lPad(cur.idx, 6, '0') || lPad(cur.month, 2, '0') || lPad(i, 5, '0');
      
      --Расчет контрольного разряда
      for j in 1..13 loop
        if (j mod 2 = 0) then
          s2 := s2 + SubStr(vkod, j, 1);
        else
          s1 := s1 + SubStr(vkod, j, 1);
        end if;
      end loop;
      
      s1 := s1 * 3;
      s3 := s1 + s2;
      k := 10 - (s3 mod 10);
      if (k = 10) then
        k := 0;
      end if;
      
      vBarcode := vKod || to_char(k);
      
      insert into barcode (id, work_id, nom_pp, barcode)
                   values (barcode_sq.nextval, aWorkId, vN, vBarcode);
    end loop;
  end loop;
  
  return Const.cResultNoError;
end FillBarcode;

end distribution_define;
/
