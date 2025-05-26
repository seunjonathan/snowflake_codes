/*--------------------------------------------
--dim_date
--simple date dimension script sourced from - https://community.snowflake.com/s/question/0D50Z00008MprP2SAJ/snowflake-how-to-build-a-calendar-dim-table
--Can also easily source a free date dimension sourced from Marketplace providers
--------------------------------------------*/

/* set the date range to build date dimension */
set min_date = to_date('2018-01-01');
set max_date = to_date('2024-12-31');
set days = (select $max_date - $min_date);

create or replace table dim_date
(
   date_id int,
   date date,
   year string, 
   month smallint,  
   month_name string,  
   day_of_month smallint,  
   day_of_week  smallint,  
   weekday string,
   week_of_year smallint,  
   day_of_year  smallint,
   weekend_flag boolean
)
as
  with dates as 
  (
    select dateadd(day, SEQ4(), $min_date) as my_date
    from TABLE(generator(rowcount=> $days))  -- Number of days after reference date in previous line
  )
  select 
        to_number(replace(to_varchar(my_date), '-')),
        my_date,
        year(my_date),
        month(my_date),
        monthname(my_date),
        day(my_date),
        dayofweek(my_date),
        dayname(my_date),
        weekofyear(my_date),
        dayofyear(my_date),
        case when dayofweek(my_date) in (0,6) then 1 else 0 end as weekend_flag
    from dates;



/*--------------------------------------------
--dim_time
--simple time dimension (hour:min:seconds) script 
--------------------------------------------*/

--set the date range to build date dimension
set min_time = to_time('00:00:00');
set max_time = to_time('11:59:59');
set seconds = 86400;


create or replace table dim_time
(
  time_id int,
  time time,
  hour smallint,   
  minute smallint,  
  second smallint,   
  am_or_pm string,   
  hour_am_pm  string  
)
as
  with seconds as 
  (
    select timeadd(second, SEQ4(), $min_time) as my_time
    from table(generator(rowcount=> $seconds))  -- Number of seconds in a day
  )
  select
         to_number(left(to_varchar(my_time), 2) || substr(to_varchar(my_time),4, 2) || right(to_varchar(my_time), 2)),
         my_time,
         hour(my_time),
         minute(my_time),
         second(my_time),
         case
            when hour(my_time) < 12 THEN 'AM'
            else 'PM'
         end as am_or_pm,
         case
             when hour(my_time) = 0 THEN '12AM'
             when hour(my_time) < 12 THEN hour(my_time) || 'AM'
             when hour(my_time) = 12 THEN '12PM'
             when hour(my_time) = 13 THEN '1PM'
             when hour(my_time) = 14 THEN '2PM'
             when hour(my_time) = 15 THEN '3PM'
             when hour(my_time) = 16 THEN '4PM'
             when hour(my_time) = 17 THEN '5PM'
             when hour(my_time) = 18 THEN '6PM'
             when hour(my_time) = 19 THEN '7PM'
             when hour(my_time) = 20 THEN '8PM'
             when hour(my_time) = 21 THEN '9PM'
             when hour(my_time) = 22 THEN '10PM'
             when hour(my_time) = 23 THEN '11PM'
         end as Hour_am_pm
    from seconds;  

