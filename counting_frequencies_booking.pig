data = LOAD '$INPUT' using PigStorage(',')		AS
			(date_time:datetime, 
			site_name:chararray,
			posa_continent:chararray,
			user_location_country:chararray,
			user_location_region:chararray,
			user_location_city:chararray,
			orig_destination_distance:chararray,
			user_id:chararray,
			is_mobile:chararray,
			is_package:chararray,
			channel:chararray,
			srch_ci:datetime,
			srch_co:datetime,
			srch_adults_cnt:chararray,	
			srch_children_cnt:chararray,
			srch_rm_cnt:chararray,
			srch_destination_id:chararray,
			srch_destination_type_id:chararray,
			is_booking:chararray,
			cnt:chararray,
			hotel_continent:chararray,
			hotel_country:chararray,
			hotel_market:chararray,
			hotel_cluster:chararray
);
data1 = filter data by is_booking  == '1' ;
extracted= foreach data1 generate  user_id,GetMonth(date_time) as mnth, GetYear(date_time) as yr;
grouped = group extracted by (yr, mnth);
counted = FOREACH grouped GENERATE group as mnth, COUNT(extracted) as cnt;
result = FOREACH counted GENERATE mnth.yr,mnth.mnth, cnt;
store result INTO '$OUTPUT';
