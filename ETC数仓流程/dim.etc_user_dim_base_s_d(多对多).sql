create table if not exists dim.etc_user_dim_base_s_d(
user_id STRING COMMENT '司机id',
etc_card_no string comment 'ETC卡号',
open_time string comment '开卡时间',
importance int comment '优先顺序'
)COMMENT 'ETC卡和用户映射基础表' partitioned by (dt string) stored as orc;


alter table dim.etc_user_dim_base_s_d drop partition(dt='${datekey}');
insert into dim.etc_user_dim_base_s_d partition(dt='${datekey}')
---2232297 司机id为测试账号
	SELECT all_user.id user_id
		  ,binding.info.card_face_number etc_card_no
		  ,binding.info.add_time open_time
		  ,2 importance
	FROM(
		SELECT id
		FROM default.dwd_logisticsqq_all_users_full
		WHERE dt='${datekey}' and type IN (1,2,3,4,21,33,34,5,6,20)
		and username not like '%贵Z%'
		and domain_id = 1
		)all_user
	join(
	    select max(named_struct('add_time',a.add_time,'card_face_number',a.card_face_number)) info
	          ,driver_id
	    from (
	    	select *
			from ods.etc_h077_open_card_order_extend 
			where dt = '${datekey}'
			and driver_id != 'null_null' ----由于老数据，为null   存在副驾的id
			)a 
	    join (
			select max(add_time) open_time
	    	,card_face_nubmer
			from ods.etc_h077_open_card_order
			where dt = date_format('${datekey}', 'yyyy-MM-dd')
				AND to_date(add_time) <= '${datekey}'
			GROUP BY card_face_nubmer
	    	)b on a.card_face_number = b.card_face_nubmer
	    group by driver_id
	    ) binding on concat(1,'_',all_user.id)= binding.driver_id;

insert into dim.etc_user_dim_base_s_d partition(dt='${datekey}')
	---车辆管理中心
	SELECT truck_user.user_id
		  ,open.card_face_nubmer etc_card_no
		  ,open.open_info.open_time open_time
		  ,3 importance
	from (
		SELECT id 
			,plate_number
			,CASE 
				WHEN plate_number_type = 0 THEN 1
				WHEN plate_number_type = 1 THEN 0
				WHEN plate_number_type = 2 THEN 4
				WHEN plate_number_type = 3 THEN 5
				WHEN plate_number_type = 4 THEN 2
				WHEN plate_number_type = 5 THEN 3
				WHEN plate_number_type = 6 THEN 6
			 END plate_number_type
		FROM ods.truck_h301_truck_truck
		WHERE dt = '${datekey}'
		  AND plate_number not like '贵Z%'
		) trucks
	JOIN (
		SELECT truck_id
			,user_id
		FROM ods.truck_h301_truck_user_truck
		WHERE dt = '${datekey}'
			AND STATUS = '1'
			AND TYPE = '1'
		) truck_user ON trucks.id = truck_user.truck_id
	join(
		SELECT id
		FROM default.dwd_logisticsqq_all_users_full
		WHERE dt='${datekey}' and type IN (1,2,3,4,21,33,34,5,6,20)
		and username not like '%贵Z%'
		and domain_id = 1
		)all_user on all_user.id = truck_user.user_id
	JOIN(
		select max(named_struct('open_time',add_time,'van_number',van_number,'van_plate_color',van_plate_color)) open_info
	    	,card_face_nubmer
			from ods.etc_h077_open_card_order
			where dt = date_format('${datekey}', 'yyyy-MM-dd')
				AND to_date(add_time) <= '${datekey}'
			GROUP BY card_face_nubmer
		)OPEN ON trucks.plate_number_type = OPEN.open_info.van_plate_color AND trucks.plate_number = OPEN.open_info.van_number;
		
insert into dim.etc_user_dim_base_s_d partition(dt='${datekey}')
	----bind_mobile 用户中心
	SELECT all_user.id user_id
		  ,binding.open_info.card_face_nubmer etc_card_no
		  ,binding.open_info.open_time open_time
		  ,1 importance
	FROM(
		SELECT id
			  ,bind_mobile phone
		FROM default.dwd_logisticsqq_all_users_full
		WHERE dt='${datekey}' and type IN (1,2,3,4,21,33,34,5,6,20)
		and username not like '%贵Z%'
		and domain_id = 1
		)all_user
	join(
		select max(named_struct('open_time',add_time,'card_face_nubmer',card_face_nubmer)) open_info
	    	,phone as open_phone
			from ods.etc_h077_open_card_order
			where dt = date_format('${datekey}', 'yyyy-MM-dd')
				AND to_date(add_time) <= '${datekey}'
			GROUP BY phone
	    ) binding on all_user.phone = binding.open_phone;

insert into dim.etc_user_dim_base_s_d partition(dt='${datekey}')
	----非平台开卡有充值记录的，用最后一次充值人是司机的记录作为对应的司机
		select deposit.operate_id as user_id,
	deposit.etc_card_no as etc_card_no,
	null as open_time,
	3 as importance
	from
	(select a.etc_card_no,a.deposit_info.deposit_time,a.deposit_info.operate_id
	from
	(select etc_card_no,max(named_struct('deposit_time',deposit_time,'operate_id',operator_id)) deposit_info
	from dwd.etc_deposit_order_fact_s_d
	where dt = '${datekey}'
	and order_status = '交易完成'
	and to_date(deposit_time) <= '${datekey}'
	group by etc_card_no) a
	join
	(
		SELECT id
		FROM default.dwd_logisticsqq_all_users_full
		WHERE dt='${datekey}' and type IN (1,2,3,4,21,33,34,5,6,20)
		and username not like '%贵Z%'
		and domain_id = 1
	) b
	on a.deposit_info.operate_id=b.id
	) deposit
	left join
	(select * from ods.etc_h077_open_card_order
			where dt = date_format('${datekey}', 'yyyy-MM-dd')
				AND to_date(add_time) <= '${datekey}') open
	on deposit.etc_card_no=open.card_face_nubmer
	where open.card_face_nubmer is null;