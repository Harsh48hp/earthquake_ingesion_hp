select time from earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet;


--- 1. Count the number of earthquakes by region

select area , count(*) number_of_earthquakes from earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet group by area order by number_of_earthquakes;

---- modifide ans with state wise


select substr(area,instr(area,',')+1,length(area)-instr(area,',')) as state , count(*) number_of_earthquakes from earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet group by state order by number_of_earthquakes desc;


---2. Find the average magnitude by the region


select area , avg(mag) avg_mag from earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet group by area order by avg_mag;

--- modified with state

select substr(area,instr(area,',')+1,length(area)-instr(area,',')) as state , avg(mag) avg_mag  from earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet group by state order by  avg_mag desc;

---3 Find how many earthquakes happen on the same day.

SELECT 
  FORMAT_TIMESTAMP('%m-%d', time) AS day_month,COUNT(*) AS number_of_earthquakes FROM earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet
GROUP BY day_month ORDER BY number_of_earthquakes DESC;






--4 Find how many earthquakes happen on same day and in same region

--- date manupulation 
SELECT area,
  FORMAT_TIMESTAMP('%m-%d', time) AS day,  count(*) as number_of_earthquake
FROM earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet group by area,day order by number_of_earthquake desc;


SELECT  substr(area,instr(area,',')+1,length(area)-instr(area,',')) as state,
  FORMAT_TIMESTAMP('%m-%d', time) AS day,  count(*) as number_of_earthquake
FROM earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet group by state,day order by number_of_earthquake desc;



-- 5 Find average earthquakes happen on the same day

SELECT 
 FORMAT_TIMESTAMP('%m-%d', time) AS day , avg(mag) as avg_earthquake from
earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet group by day order by day desc;




--- 6 Find average earthquakes happen on same day and in same region

SELECT area,
  FORMAT_TIMESTAMP('%m-%d', time) AS day, avg(mag) as avg_earthquake
FROM earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet group by area,day order by day desc;



SELECT substr(area,instr(area,',')+1,length(area)-instr(area,',')) as state,
  FORMAT_TIMESTAMP('%m-%d', time) AS day, avg(mag) as avg_earthquake
FROM earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet group by state,day order by day desc;




--- 7 Find the region name, which had the highest magnitude earthquake last week.

select area,time  from earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet where mag = (select max(mag) from 
earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet);

-----
select area, time,mag from earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet
WHERE mag = (SELECT MAX(mag) FROM earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet 
WHERE time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
) AND time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) ;
----

WITH last_seven_days AS (
    SELECT area, time, mag
    FROM earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet
    WHERE time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)

SELECT area, time, mag
FROM last_seven_days
WHERE mag = (SELECT MAX(mag) FROM last_seven_days);


--- 8 Find the region name, which is having magnitudes higher than 5.

select area,mag from earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet where mag > 5;


--- 9 Find out the regions which are having the highest frequency and intensity of earthquakes.

SELECT area,COUNT(*) AS frequency,MAX(mag) AS max_intensity FROM earthquake-analysis-440806.earthquake_analysis.new_flattened_historical_data_by_parquet
GROUP BY area ORDER BY frequency DESC, max_intensity DESC;