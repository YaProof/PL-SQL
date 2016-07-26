select tab1.pn as "Номер телефона",
       nvl(tab1.rm, case
                      when tab1.prm1 is not null then (select pic1.filename from image_files pic1 where pic1.id = tab1.prm1)
                      when tab1.prm2 is not null then (select pic2.filename from image_files pic2 where pic2.id = tab1.prm2)
                      when tab1.prm3 is not null then (select pic3.filename from image_files pic3 where pic3.id = tab1.prm3)
                      when tab1.prm4 is not null then (select pic4.filename from image_files pic4 where pic4.id = tab1.prm4)
                      when tab1.prm5 is not null then (select pic5.filename from image_files pic5 where pic5.id = tab1.prm5)
                      when tab1.prm6 is not null then (select pic6.filename from image_files pic6 where pic6.id = tab1.prm6)
                      when tab1.prm7 is not null then (select pic7.filename from image_files pic7 where pic7.id = tab1.prm7)
                      when tab1.prm8 is not null then (select pic8.filename from image_files pic8 where pic8.id = tab1.prm8)
                      when tab1.prm9 is not null then (select pic9.filename from image_files pic9 where pic9.id = tab1.prm9)
                      when tab1.prm10 is not null then (select pic10.filename from image_files pic10 where pic10.id = tab1.prm10)
                    end
          ) as "Рекламный модуль",
       '4'  as "Номер РП"
 from
(
    select d.client_id as pn,--"Номер телефона",
         case
           when tp.prm_id = 1  then img1.filename
           when tp.prm_id = 2  then img2.filename
           when tp.prm_id = 3  then img3.filename
           when tp.prm_id = 4  then img4.filename
           when tp.prm_id = 5  then img5.filename
           when tp.prm_id = 6  then img6.filename
           when tp.prm_id = 7  then img7.filename
           when tp.prm_id = 8  then img8.filename
           when tp.prm_id = 9  then img9.filename
           when tp.prm_id = 10 then img10.filename
         end  as rm --"Рекламный модуль",
         , d.prm1
         , d.prm2
         , d.prm3
         , d.prm4
         , d.prm5
         , d.prm6
         , d.prm7
         , d.prm8
         , d.prm9
         , d.prm10
      from doc d
        join stat tp                on d.document_id = tp.document_id
        left join image_files img1  on d.prm1        = img1.id
        left join image_files img2  on d.prm2        = img2.id
        left join image_files img3  on d.prm3        = img3.id
        left join image_files img4  on d.prm4        = img4.id
        left join image_files img5  on d.prm5        = img5.id
        left join image_files img6  on d.prm6        = img6.id
        left join image_files img7  on d.prm7        = img7.id
        left join image_files img8  on d.prm8        = img8.id
        left join image_files img9  on d.prm9        = img9.id
        left join image_files img10 on d.prm10       = img10.id
      where 1=1
        and d.id in (300, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313)
) tab1;
