data = LOAD '$INPUT'
	USING PigStorage(',')		AS
			(date_time:chararray, 
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
			hotel_continent:chararray,
			hotel_country:chararray,
			hotel_market:chararray,
			is_booking:chararray,
			cnt:chararray,
			hotel_cluster:chararray
);

 grouped = group data by channel;
 counted = FOREACH grouped GENERATE group as channel, COUNT(data) as cnt;
 result = FOREACH counted GENERATE channel, cnt;
 store result INTO '$OUTPUT';
 

