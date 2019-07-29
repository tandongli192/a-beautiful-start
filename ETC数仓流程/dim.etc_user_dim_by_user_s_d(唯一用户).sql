create table if not exists dim.etc_user_dim_by_user_s_d(
user_id STRING COMMENT '司机id',
etc_card_no string comment 'ETC卡号',
open_time string comment '开卡时间'
)COMMENT 'ETC卡和用户映射唯一用户表' partitioned by (dt string) stored as orc;

alter table dim.etc_user_dim_by_user_s_d drop partition(dt='${datekey}');
insert into dim.etc_user_dim_by_user_s_d partition(dt='${datekey}')
	select user_id
		  ,etc_card_no
		  ,open_time
	from (
		select *
			  ,row_number() over (partition by user_id order by importance desc,open_time desc) rownum 
		from dim.etc_user_dim_base_s_d where dt='${datekey}'
		) x 
	where x.rownum = 1;