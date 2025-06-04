/* set the date parameters and range to build date dimension */
ALTER SESSION SET WEEK_OF_YEAR_POLICY = 1;
ALTER SESSION SET WEEK_START = 1;
set min_date = to_date('2010-01-01');
set max_date = to_date('2030-12-31');
set days = (select $max_date - $min_date);


create or replace temp table dim_date
(
	DATE_ID NUMBER(38,0),
	DATE DATE,
	DAY_OF_MONTH VARCHAR(20),
	DAY_NAME VARCHAR(10),
	DAY_OF_WEEK NUMBER(2,0),
	DAY_OF_WEEK_IN_MONTH NUMBER(9,0),
	DAY_OF_QUARTER NUMBER(10,0),
	DAY_OF_YEAR VARCHAR(20),
	WEEK_OF_YEAR VARCHAR(20),
	WEEK_OF_MONTH NUMBER(9,0),
	WEEK_OF_QUARTER NUMBER(17,0),
	MONTH VARCHAR(20),
	MONTH_NAME VARCHAR(10),
	MONTH_OF_QUARTER NUMBER(5,0),
	QUARTER NUMBER(2,0),
	QUARTER_NAME VARCHAR(20),
	YEAR VARCHAR(20),
	MONTH_YEAR VARCHAR(20),
	MMYYYY VARCHAR(20),
	FIRST_DAY_OF_MONTH DATE,
	LAST_DAY_OF_MONTH DATE,
	FIRST_DAY_OF_QUARTER DATE,
	LAST_DAY_OF_QUARTER DATE,
	FIRST_DAY_OF_YEAR DATE,
	WEEKEND_FLAG BOOLEAN

) 
as
with dates as (
  select dateadd(day, seq4(), $min_date) as my_date
  from table(generator(rowcount => $days))
),
main_rows as (
  select
    to_number(replace(to_varchar(my_date), '-')) as date_id,
    my_date as date,

    -- Day-related
    day(my_date) as day_of_month,
    dayname(my_date) as day_name,
    dayofweek(my_date) as day_of_week, -- Sunday=1, Saturday=7
    ceil(day(my_date) / 7.0) as day_of_week_in_month,
    datediff(day, date_trunc(quarter, my_date), my_date) + 1 as day_of_quarter,
    dayofyear(my_date) as day_of_year,

    -- Week-related
    weekofyear(my_date) as week_of_year,
    ceil(day(my_date) / 7.0) as week_of_month,
    ceil((datediff(day, date_trunc(quarter, my_date), my_date) + 1) / 7.0) as week_of_quarter,

    -- Month/Quarter/Year
    month(my_date) as month,
    monthname(my_date) as month_name,
    month(my_date) - (quarter(my_date) - 1) * 3 as month_of_quarter,
    quarter(my_date) as quarter,
    case quarter(my_date)
        when 1 then 'First'
        when 2 then 'Second'
        when 3 then 'Third'
        when 4 then 'Fourth'
    end as quarter_name,

    year(my_date) as year,
    to_char(my_date, 'MON-YYYY') as month_year,
    to_char(my_date, 'MMYYYY') as mmyyyy,

    -- Anchors
    date_trunc(month, my_date) as first_day_of_month,
    last_day(my_date, 'month') as last_day_of_month,
    date_trunc(quarter, my_date) as first_day_of_quarter,
    last_day(my_date, 'quarter') as last_day_of_quarter,
    date_trunc(year, my_date) as first_day_of_year,

    -- check weekend
    case when dayofweek(my_date) in (6,7) then true else false end as weekend_flag
  from dates
),
/* factor in null dates when joined with fact tables */
null_row as (
  select
    -1 as date_id,
    null::date as date,
    null as day_of_month,
    'Unknown' as day_name,
    null as day_of_week,
    null as day_of_week_in_month,
    null as day_of_quarter,
    null as day_of_year,
    null as week_of_year,
    null as week_of_month,
    null as week_of_quarter,
    null as month,
    'Unknown' as month_name,
    null as month_of_quarter,
    null as quarter,
    'Unknown' as quarter_name,
    null as year,
    'Unknown' as month_year,
    '000000' as mmyyyy,
    null::date as first_day_of_month,
    null::date as last_day_of_month,
    null::date as first_day_of_quarter,
    null::date as last_day_of_quarter,
    null::date as first_day_of_year,
    null as weekend_flag
)

select * from main_rows
union all
select * from null_row;
