-- https://www.kaggle.com/datasets/swaptr/layoffs-2022   -Dataset Source


-- Data cleaning

select* from Layoffs;
use world_layoffs;
-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

create table layoff_staging 
like layoffs;

select * from layoff_staging;

insert layoff_staging
select * from layoffs; 

select *, 
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,`date`) as row_num
from layoff_staging;

with duplicate_cte as
(
select *, 
row_number() over(
partition by company, location, 
industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoff_staging
)
select * 
from duplicate_cte
where row_num>1;
 
select * from layoff_staging 
where company = "casper";

with duplicate_cte as
(
select *, 
row_number() over(
partition by company, location, 
industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoff_staging
)
delete 
from duplicate_cte
where row_num>1; 


CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoff_staging2; 

insert into layoff_staging2
select *, 
row_number() over(
partition by company, location, 
industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised_millions) as row_num
from layoff_staging;

select * from layoff_staging2
where row_num>1;

delete from layoff_staging2
where row_num>1;

select * from layoff_staging2; 

-- Standardizing Data

-- Trim the blanck space in company column
select company, trim(company)
from layoff_staging2;

Update layoff_staging2
set company = Trim(Company);

select distinct(industry)
from layoff_staging2
order by 1;

-- Fix the Crypto name(where it was cryptocurrency)
select * from layoff_staging2
where industry like "crypto%";

update layoff_staging2
set industry = "crypto"
where industry like "crypto%";

select * from layoff_staging2;

select distinct (country)
from layoff_staging2
order by 1;

select distinct(country), trim(trailing '.' from country)
from layoff_staging2
order by 1;

update layoff_staging2
set country = trim(trailing '.' from country)
where country like "United States%";

SELECT *
FROM layoff_staging2
WHERE location LIKE 'Flor%'; 

update layoff_staging2
set location = 'Florianopolis'
WHERE location LIKE 'Flor%';

SELECT *
FROM layoff_staging2
WHERE location LIKE '%sseldorf%'; 

update layoff_staging2
set location = 'Dusseldorf'
WHERE location LIKE '%sseldorf%';

-- Fixing Date datatype and Format
select `date`,
str_to_date(`date`,'%m/%d/%Y') 
from layoff_staging2;

update layoff_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoff_staging2
modify column `date` date;

select distinct location from layoff_staging2 order by 1;

-- Checking for Null and Blank values in Rows
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. so I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase
-- so there isn't anything I want to change with the null values

select * 
from layoff_staging2
where total_laid_off is Null
and percentage_laid_off is Null;

select * from layoff_staging2
where industry is null 
or industry= '';

select * 
from layoff_staging2
where company like'bally%';

select t1.industry, t2.industry
from layoff_staging2 t1
inner join layoff_staging2 t2
on t1.company=t2.company 
where (t1.industry is null or t1.industry='')
and t2.industry is not null; 

-- Populating Null values in industry

update layoff_staging2
set industry = null
where industry = '';

update layoff_staging2 t1
join layoff_staging2 t2
    on t1.company= t2.company
set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;

-- Removing any columns and rows that are not necessary 
select * 
from layoff_staging2
where total_laid_off is Null
and percentage_laid_off is Null;

delete 
from layoff_staging2
where total_laid_off is Null
and percentage_laid_off is Null;


select * 
from layoff_staging2;

alter table layoff_staging2
drop column row_num;

-- Explotary Data Analysis

select * from layoff_staging2;

select max(total_laid_off), max(percentage_laid_off) 
from layoff_staging2;

select * 
from layoff_staging2
where percentage_laid_off =1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoff_staging2
group by company
order by 2 desc;

select min(`date`), max(date)
from layoff_staging2;

-- which industry affected the most

select industry, sum(total_laid_off)
from layoff_staging2
group by industry
order by 2 desc;

select * from layoff_staging2;

-- which Country affected the most

select country, sum(total_laid_off)
from layoff_staging2
group by country
order by 2 desc;

-- which year affected the most

select year(date), sum(total_laid_off)
from layoff_staging2
group by year(date)
order by 1 desc;

-- which stage affected the most

select stage, sum(total_laid_off)
from layoff_staging2
group by stage
order by 2 desc;

SELECT stage, ROUND(AVG(percentage_laid_off),2)
FROM layoff_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Earlier we looked at Companies with the most Layoffs. Now lets look at that per year
-- Rolling Total of Layoffs Per Month

select substring(`date`,1,7) as Month, sum(total_laid_off)
from layoff_staging2
where substring(`date`,1,7) is not null
group by month
order by 1 asc;

with Rolling_total as (
select substring(`date`,1,7) as Month, sum(total_laid_off) as total_off
from layoff_staging2
where substring(`date`,1,7) is not null
group by month
order by 1 asc
)
select month, total_off,
sum(total_off) over(order by month ) as rolling_total
from Rolling_total;

-- Earlier we looked at Companies with the most Layoffs. Now lets look at that per year

select company, year(`date`), sum(total_laid_off)
from layoff_staging2
group by company, year(`date`)
order by 3 desc;


with company_year as 
(
select company, year(`date`) as Year, sum(total_laid_off) as Total_laid_off
from layoff_staging2
group by company, year(`date`)
),
 company_year_rank as
(
select *, dense_rank() over (partition by year order by Total_laid_off desc) as Ranking
from company_year
where year is not null
)
select *
from company_year_rank
where ranking <=5;

