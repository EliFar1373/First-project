create table tr(Id int);

--Creating dimension tables

CREATE TABLE dbo.DimCustomer
(
    CustomerKey INT IDENTITY NOT NULL,
    CustomerAlternateKey NVARCHAR(15) NULL,
    CustomerName NVARCHAR(80) NOT NULL,
    EmailAddress NVARCHAR(50) NULL,
    Phone NVARCHAR(25) NULL,
    StreetAddress NVARCHAR(100),
    City NVARCHAR(20),
    PostalCode NVARCHAR(10),
    CountryRegion NVARCHAR(20)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);



CREATE TABLE dbo.DimGeography
(
    GeographyKey INT IDENTITY NOT NULL,
    GeographyAlternateKey NVARCHAR(10) NULL,
    StreetAddress NVARCHAR(100),
    City NVARCHAR(20),
    PostalCode NVARCHAR(10),
    CountryRegion NVARCHAR(20)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dbo.DimCustomer2
(
    CustomerKey INT IDENTITY NOT NULL,
    CustomerAlternateKey NVARCHAR(15) NULL,
    GeographyKey INT NULL,
    CustomerName NVARCHAR(80) NOT NULL,
    EmailAddress NVARCHAR(50) NULL,
    Phone NVARCHAR(25) NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

--Time dimension tables

CREATE TABLE dbo.DimDate
( 
    DateKey INT NOT NULL,
    DateAltKey DATETIME NOT NULL,
    DayOfMonth INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName NVARCHAR(15) NOT NULL,
    MonthOfYear INT NOT NULL,
    MonthName NVARCHAR(15) NOT NULL,
    CalendarQuarter INT  NOT NULL,
    CalendarYear INT NOT NULL,
    FiscalQuarter INT NOT NULL,
    FiscalYear INT NOT NULL
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);


CREATE TABLE dbo.FactSales
(
    CustomerKey INT NOT NULL,
    GeographyKey INT IDENTITY NOT NULL,
    DateKey INT NOT NULL
    
    
     
)
WITH
(
    DISTRIBUTION = HASH(CustomerKey),
    CLUSTERED COLUMNSTORE INDEX
);




--create saging table 

create table dbo.StageCustomer(
    CustomerKey INT IDENTITY NOT NULL,
    CustomerAlternateKey NVARCHAR(15) NULL,
    CustomerName NVARCHAR(80) NOT NULL,
    EmailAddress NVARCHAR(50) NULL,
    Phone NVARCHAR(25) NULL,
    StreetAddress NVARCHAR(100),
    City NVARCHAR(20),
    PostalCode NVARCHAR(10),
    CountryRegion NVARCHAR(20)

)WITH
(DISTRIBUTION=ROUND_ROBIN,
CLUSTERED COLUMNSTORE index
);


--create external table

---1:crare external data source
create EXTERNAL DATA SOURCE stagefile
with(
    LOCATION='https://gen2resource3.dfs.core.windows.net/file1/'
);
GO

--2:create external file format
create EXTERNAL FILE FORMAT parqueformat
with(
    FORMAT_TYPE=PARQUET
);
GO
---3:create external table
create external table dbo.ExternalstageCustomer(
    [model] nvarchar(4000),
	[mpg] float,
	[cyl] int,
	[disp] float,
	[hp] int,
	[drat] float,
	[wt] float,
	[qsec] float,
	[vs] int,
	[am] int,
	[gear] int,
	[carb] int


)with(
    DATA_SOURCE=stagefile,
    LOCATION='mtcars.parquet',
    File_FORMAT=parqueformat

);
GO




--load data to wharehause
create table dbo.StageCar(
    [model] nvarchar(4000),
	[mpg] float,
	[cyl] int,
	[disp] float,
	[hp] int,
	[drat] float,
	[wt] float,
	[qsec] float,
	[vs] int,
	[am] int,
	[gear] int,
	[carb] int

)WITH
(DISTRIBUTION=ROUND_ROBIN,
CLUSTERED COLUMNSTORE index
);

copy into dbo.StageCar(model,mpg,cyl,disp,hp,drat,wt,qsec,vs,am,gear,carb)
from 'abfss://file1@gen2resource3.dfs.core.windows.net/mtcars.parquet'
with(
     FILE_TYPE='PARQUET',
     MAXERRORS = 0,
    IDENTITY_INSERT = 'OFF'
);


-- create stage table, copy data from lake into stage table, craete tabl and transfer data from stage to real table
create table dbo.stagetest(

    [ORDERNUMBER] bigint,
	[QUANTITYORDERED] bigint,
	[PRICEEACH] float
)
with(
    DISTRIBUTION=ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX

);

COPY INTO dbo.stagetest(ORDERNUMBER,QUANTITYORDERED,PRICEEACH)
from 'https://gen2resource3.dfs.core.windows.net/file1/sales_short.csv'
with(
    FILE_TYPE='CSV',
    FIRSTROW = 2

);

select top 5* from dbo.stagetest;


create table dbo.tabletest(
    [ORDERNUMBER] bigint,
	[QUANTITYORDERED] bigint,
	[PRICEEACH] float

)with(
    DISTRIBUTION=REPLICATE,
    CLUSTERED COLUMNSTORE INDEX


)



-- Step 5: Transfer data from the staging table to the real table , 
insert into dbo.tabletest(ORDERNUMBER,QUANTITYORDERED,PRICEEACH)
SELECT ORDERNUMBER,QUANTITYORDERED,PRICEEACH from dbo.stagetest;


select top 5* from dbo.tabletest;




----second approach but it did not work
---- Use MERGE to transfer and update data

create table dbo.tabletest1(
    [ORDERNUMBER] bigint,
	[QUANTITYORDERED] bigint,
	[PRICEEACH] float

)with(
    DISTRIBUTION=REPLICATE,
    CLUSTERED COLUMNSTORE INDEX


);

---add surrogate key to both table1 and stage table:
Alter table dbo.tabletest1 add Surrogatekey int identity(1,1); -- Add a surrogate key with auto-increment
Alter table dbo.stagetest add Surrogatekey int identity(1,1);





MERGE INTO dbo.tabletest1 AS target  -- Real table (target)
USING dbo.stagetest AS source         -- Staging table (source)
ON target.ORDERNUMBER = source.ORDERNUMBER   -- Matching condition on ORDERNUMBER

-- Step 4: Update existing rows in the target table
WHEN MATCHED THEN
    UPDATE SET
        target.QUANTITYORDERED = source.QUANTITYORDERED,   -- Update the quantity
        target.PRICEEACH = source.PRICEEACH,               -- Update the price
        target.UPDATED_AT = GETDATE();                     -- Track when the record was updated

when not matched by target then
insert (ORDERNUMBER, QUANTITYORDERED, PRICEEACH)
values(Source.ORDERNUMBER, Source.QUANTITYORDERED, Source.PRICEEACH);







---quary data:
select CustomerName, City,Po
from dbo.DimCustomer2 as cus
join  dbo.DimGeography as geo
on cus.GeographyAlternateKey=geo.GeographyAlternateKey;

select CustomerName,City,CountryRegion, count(CustomerName) as d
from dbo.DimCustomer2 as cus
join dbo.DimGeography as geo
on cus.GeographyKey=geo.GeographyAlternateKey
group by  CustomerName,City,CountryRegion
order by CountryRegion;


select CustomerName,CountryRegion, count(CustomerName) as numbe_of_customers
from dbo.DimCustomer2 as cus
join dbo.DimGeography as geo
on cus.GeographyKey=geo.GeographyAlternateKey
group by CountryRegion, CustomerName
order by CountryRegion;



----APPROX_COUNT_DISTINCT

select CountryRegion, APPROX_COUNT_DISTINCT(CustomerName) as number
from dbo.DimCustomer2 as cus
join dbo.DimGeography as geo
on cus.GeographyKey=geo.GeographyAlternateKey
group by CountryRegion;


select sum(PRICEEACH) as totalPrice from dbo.tabletest;




----data wharehausing using external table:


create external file FORMAT filetestCSV1
with(
   
    FORMAT_TYPE=DELIMITEDTEXT,
    FORMAT_OPTIONS(
        
        FIELD_TERMINATOR = ',',
        FIRST_ROW=2)
    
    );



-- Create a master key in the database
create master key ENCRYPTION by PASSWORD='Elham$$123';

-- Create a database scoped credential for the SAS token
create database SCOPED credential mycredencial1
with
    IDENTITY = 'SHARED ACCESS SIGNATURE', 
    SECRET = 'https://gen2resource3.blob.core.windows.net/file1/sales_short.csv?sp=racwdymeop&st=2024-12-02T01:03:59Z&se=2024-12-28T09:03:59Z&spr=https&sv=2022-11-02&sr=b&sig=2idtEQpWKEbgk1CzQV%2FCjCk0eV5xkiHCs%2F8r1wGdXe4%3D'
  

create external data SOURCE srtest6 
with(
    location='abfss://file1@gen2resource3.dfs.core.windows.net/',

    CREDENTIAL=mycredencial1
  
    );


create external table exttest(
    [ORDERNUMBER] bigint,
	[QUANTITYORDERED] bigint,
	[PRICEEACH] float
)
with(
   -- DATA_SOURCE = srtest2,
    LOCATION = 'abfss://file1@gen2resource3.dfs.core.windows.net/sales_short.csv' -- File or folder path
   --- FILE_FORMAT = filetestCSV
    

);



create database SCOPED credential mycredencial1
with
    IDENTITY = 'SHARED ACCESS SIGNATURE', 
    SECRET = 'https://gen2resource3.blob.core.windows.net/file1/sales_short.csv?sp=r&st=2024-12-02T01:23:12Z&se=2024-12-02T09:23:12Z&skoid=7d41dc8c-e492-4c86-b2f8-572f42e99190&sktid=bdb74b30-9568-4856-bdbf-06759778fcbc&skt=2024-12-02T01:23:12Z&ske=2024-12-02T09:23:12Z&sks=b&skv=2022-11-02&spr=https&sv=2022-11-02&sr=b&sig=Ryoqlzrgi6%2Fvm6khJht1N%2FhVoWYdEh029CZaFRdXO7E%3D'

CREATE EXTERNAL DATA SOURCE srtest9
WITH (
    LOCATION = 'abfss://file1@gen2resource3.dfs.core.windows.net',  -- This should point to the root of the container
    CREDENTIAL = mycredencial1
);




create external table exttest3(
    [ORDERNUMBER] bigint,
	[QUANTITYORDERED] bigint,
	[PRICEEACH] float
)
with(
    DATA_SOURCE = srtest9,
    LOCATION = 'sales_short.csv',-- File or folder path
    FILE_FORMAT = filetestCSV
    

);


----- load data into a new dimension table is to use a CREATE TABLE AS (CTAS) 
select top 4* from dbo.stagetest;

create table dbo.dimtabletestCTAS
with(
    DISTRIBUTION=REPLICATE,
    CLUSTERED COLUMNSTORE INDEX

)
as 
select [ORDERNUMBER] ,
	[QUANTITYORDERED],
	[PRICEEACH] from dbo.stagetest; 








