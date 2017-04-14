data = LOAD '$INPUT' 	USING PigStorage(',')			AS			(date_time:datetime, 			site_name:chararray,			posa_continent:chararray,			user_location_country:chararray,			user_location_region:chararray,			user_location_city:chararray,			orig_destination_distance:chararray,			user_id:chararray,			is_mobile:chararray,			is_package:chararray,			channel:chararray,			srch_ci:datetime,			srch_co:datetime,			srch_adults_cnt:chararray,				srch_children_cnt:chararray,			srch_rm_cnt:chararray,			srch_destination_id:chararray,			srch_destination_type_id:chararray,	is_booking:chararray,			count:chararray,			hotel_continent:chararray,			hotel_country:chararray,			hotel_market:chararray,					hotel_cluster:chararray); 


data1 = filter data by srch_ci is not null ;

daysbetween_data = foreach data1 generate  user_id, DaysBetween(srch_ci,date_time) as btwn;
grouped =  group  daysbetween_data by user_id;
unordered = FOREACH grouped GENERATE group as user_id, AVG(daysbetween_data.btwn) as cmp;
ordered = order unordered by cmp; 
result = FOREACH ordered GENERATE user_id, cmp, ((int)cmp>=15 ? 'L' : 'B' ) as v;
 store result INTO '$OUTPUT';
 
