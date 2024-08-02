--------------------- SQL DATA CLEANINIG (DC) AND EXPLORATORY DATA ANALYSIS (EDA) ----------------------------------------

----------------------- WORLD LAYOFFS SQL PROJECT 2------------------------------------------
-------------------------------  SQL DATA CLEANINIG (DC) -------------------------------------

SELECT *
FROM world_layoffs;

---------------------------- creating a new worksheet ------------------
create table layoffs
like world_layoffs;

insert into layoffs 
select *
from world_layoffs ;

select * 
from layoffs ;

------------------------ trim columns ------------------------------------
select company ,trim(company)
from layoffs;

update layoffs 
set company =trim(company) ;

select location , trim(location)
from layoffs ;

update layoffs 
set location =trim(location);

select industry, location, trim(industry) , trim(location)
from layoffs; 

update layoffs 
set industry= trim(industry);

select location, trim(location)
from layoffs;

update layoffs 
set location =trim(location);

------------------------- removing duplicates --------------------

--- create a new unique coulumn (row_num) and put it in a cte ------
select *,
row_number()
over(partition by company,location,industry,total_laid_off,percentage_laid_off,"date",stage,country,funds_raised_millions) as row_num
from layoffs;

with duplicate_cte as
(select *,
row_number()
over(partition by company,location,industry,total_laid_off,percentage_laid_off,"date",stage,country,funds_raised_millions) as row_num
from layoffs
)
select *
from duplicate_cte
where row_num >1;

------- we create a new table layoffs_2 ---------
# right click on the layoffs table (worksheet table) , through  copy to clipboard click on " create statment
# and paste on booard
# then we add a new coumn (row_num)

CREATE TABLE `layoffs_2` (
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

select *
from layoffs_2;

--------- insert data into layoffs_2---------- 
insert into layoffs_2
select *,
row_number()
over(partition by company,location,industry,total_laid_off,percentage_laid_off,"date",stage,country,funds_raised_millions) as row_num
from layoffs;

select *
from layoffs_2
where row_num >1;

delete 
from layoffs_2
where row_num >1;

select *
from layoffs_2;

---------------------------- standardizing ----------------------------------------------------
----- spelling errors ----
select *
from layoffs_2;

select distinct company
from layoffs_2
order by 1;
----
select distinct location
from layoffs_2
order by 1;
----
select distinct industry
from layoffs_2
order by 1;

select industry
from layoffs_2
where industry like "crypto%";

select distinct industry
from layoffs_2
where industry like "crypto%";

update layoffs_2
set industry = "Crypto"
where industry like "crypto%";
----

select distinct country
from layoffs_2
order by 1;

select country
from layoffs_2
where country like "united states%";

update layoffs_2
set country = "United states"
where country like "united states%";

------ converting text to date -------

select date
 from layoffs_2;
 
 select date,
 str_to_date(date,"%m/%d/%Y")
 from layoffs_2;
 
 update layoffs_2
 set date = str_to_date(date,"%m/%d/%Y");
 
 alter table layoffs_2
 modify column date date;
 
---------------------------- removing null and blank values -------------------------
select * 
from layoffs_2;

select  industry, company, location
from layoffs_2
order by 1;

select  industry, company, location, total_laid_off
from layoffs_2
where company = "Bally's Interactive";

select  industry, company, location, total_laid_off
from layoffs_2
where company = "airbnb";

update layoffs_2
set industry = "Travel"
where total_laid_off =30;

select  industry, company, location, total_laid_off
from layoffs_2
where company = "carvana";

update layoffs_2
set industry = "Transportation"
where total_laid_off =2500;

select  industry, company, location, total_laid_off
from layoffs_2
where company = "juul";

update layoffs_2
set industry = "Consumer"
where total_laid_off =400;

----- deleting unwanted null values or rows ----
select*
from layoffs_2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_2
where total_laid_off is null
and percentage_laid_off is null;

----------------- removing columns ------------------
select * 
from layoffs_2;

alter table layoffs_2
drop column row_num;


--------------------------------------- EXPLORATORY DATA ANALYSIS (EDA) --------------------------------------------
--- view table 
select * 
from layoffs_2;

--- top ten countries with the highest number of laid off and fund raised(in million)
select country,sum(total_laid_off)as sum_laid_off,sum(funds_raised_millions) as sum_funds_raised_millions
from layoffs_2
group by country
order by sum_laid_off desc,sum_funds_raised_millions desc
limit 10;

--- insights --we can see that united states leds as the country with the most laid off staffs despite the fact that they raised the most fund 
--- china and singapore  bottoms the table as the countries with the most laid off staffs in the top ten laid off table 

--- top ten location  with the highest number of laid off and fund raised(in million)
select location,country,sum(total_laid_off)as sum_laid_off,sum(funds_raised_millions) as sum_funds_raised_millions
from layoffs_2
group by location,country
order by sum_laid_off desc,sum_funds_raised_millions desc
limit 10;

select country,sum(sum_laid_off),sum(sum_funds_raised_millions)
from
(select location,country,sum(total_laid_off)as sum_laid_off,sum(funds_raised_millions) as sum_funds_raised_millions
from layoffs_2
group by location,country
order by sum_laid_off desc,sum_funds_raised_millions desc
limit 10) as top_10
where country = 'united states';

--- insights--- locations where most staffs were laid off are from the united states appearing six times on the top 10 table 
--- locations in the  united states on the top 10 table made above 215k laid off

--- top ten and least industries  with the highest and lowest  number of laid off and fund raised(in million)
select industry,sum(total_laid_off)as sum_laid_off,sum(funds_raised_millions) as sum_funds_raised_millions
from layoffs_2
group by industry
order by sum_laid_off desc,sum_funds_raised_millions desc
limit 10;

select industry,sum(total_laid_off)as sum_laid_off,sum(funds_raised_millions) as sum_funds_raised_millions
from layoffs_2
group by industry
order by sum_laid_off ,sum_funds_raised_millions 
limit 11;

--- insights--- the consumer industry had the most laid off closely followed by the retail industry
-- while the manufacturing industry had the least laid off staff followed  by the fin-tech industry;

-- top ten  company  with the highest  number of laid off and fund raised(in million)
select company,industry,sum(total_laid_off)as sum_laid_off,sum(funds_raised_millions) as sum_funds_raised_millions
from layoffs_2
group by company,industry
order by sum_laid_off desc,sum_funds_raised_millions desc
limit 10;

--- insights--- amazon  leads as the company with the most laid off staff followed by google and meta which are both consumers and salesforce at fourth pole 
