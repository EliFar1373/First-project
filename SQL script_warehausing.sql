--create dimension time table
create table dbo.Dmdate2(
    calenderDay date not null,
    dayy int,
    DateKey int
    
    );

    ------run all code till end of go together
-- Create a temporary table for the dates we need
create table #TmpstageDate(datec DATE NOT NULL);

-- Populate the temp table with a range of dates
declare @Startdate DATE
declare @Enddate DATE
declare @Loopdate DATE
SET @Startdate='2024-12-02'
set @Enddate='2025-12-02'
set @Loopdate =@Startdate
 
while @Loopdate <=@Enddate
begin 
insert into #TmpstageDate VALUES(@Loopdate)
set @Loopdate=DATEADD(dd,1,@Loopdate)
end


-- Insert the dates and calculated attributes into the dimension table

--insert into dbo.Dmdate

insert into dbo.Dmdate2 (calenderDay ,dayy ,DateKey )
select datec ,DAY(datec) ,
 CAST(convert(varchar(20),datec,112)as int) 
from #TmpstageDate;
GO

--Drop temporary table
DROP TABLE #TmpStageDate


select top(7)* from dbo.Dmdate2;




----Combining INSERT and UPDATE statements

select top(4)* from dbo.stagetest;

drop table dbo.tabletest3;

create table dbo.tabletest3(
    ordernum BIGINT null,
    quan bigint null,
    price float null


)
with(
    DISTRIBUTION=HASH(ordernum),
    CLUSTERED COLUMNSTORE index
);
ALTER table dbo.tabletest3 add surkey int null null;

insert into dbo.tabletest3(ordernum,quan,price,surkey)
select ORDERNUMBER,QUANTITYORDERED,PRICEEACH,Surrogatekey
from dbo.stagetest;

-- Type 1 updates (quan)
--imagine we have secodn stage table and new one
update dbo.tabletest3 
set dbo.tabletest3.quan=st.QUANTITYORDERED
from dbo.stagetest as st
where dbo.tabletest3.surkey=st.Surrogatekey;

---Type 2 updates (StreetAddress)
insert into dbo.tabletest3 (ordernum,quan,price,surkey)
select ORDERNUMBER,QUANTITYORDERED,PRICEEACH,Surrogatekey from dbo.stagetest as st
join dbo.tabletest3 as tb3
on st.Surrogatekey=tb3.surkey
WHERE tb3.quan<>st.QUANTITYORDERED;




---Using a MERGE statement  ,you need hash distribution for merge for target table

Merge  dbo.tabletest3 as tb3
using (select * from dbo.stagetest) as st
on st.Surrogatekey=tb3.surkey
when matched then
-- Type 1 updates
update set 
tb3.ordernum=st.ORDERNUMBER,
tb3.price=st.PRICEEACH,
tb3.quan=st.QUANTITYORDERED

when not matched then
 -- New products type2
 insert  (ordernum, quan, price, surkey)
  values(
 st.ORDERNUMBER,
 st.QUANTITYORDERED,
 st.PRICEEACH,
 st.Surrogatekey);


create STATISTICS newStat
on dbo.tabletest3(price);

DBCC SHOW_STATISTICS ('dbo.tabletest3', 'newStat');

select * from dbo.tabletest3  ;

----indexing
create NONCLUSTERED index index_1
on dbo.tabletest3(quan);

CREATE NONCLUSTERED INDEX index_2_filtering
ON dbo.tabletest3(price)
WHERE price < 100;


create unique nonclustered index index3
on dbo.tabletest3(ordernum);

----maintanace index
alter INDEX all on dbo.tabletest3 rebuild;





