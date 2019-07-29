
create table if not exists dws.etc_core_performance_acitve_s_d(
	etc_card STRING COMMENT'ETC卡号',
	card_name STRING COMMENT'卡类型',
	active_amount int comment '激活充值金额',
	perf_active_type INT COMMENT'业绩激活类型(1员工操作充值激活/2卖套餐激活/3自主激活)',
	perf_active_combo_ord STRING COMMENT'业绩激活时套餐订单号',
	perf_active_combo_code STRING COMMENT'业绩激活时套餐编码',
	perf_active_combo_name STRING COMMENT'业绩激活时套餐名',
	perf_active_time STRING COMMENT'业绩激活时间',
	perf_active_emp_user_id STRING COMMENT'业绩激活员工user_id', 
	perf_active_emp_name STRING COMMENT'业绩激活员工名字', 
	perf_active_emp_work_no STRING COMMENT'业绩激活员工工号', 
	perf_active_emp_ymm_work_no STRING COMMENT'业绩激活员工YMM工号',
	perf_active_emp_area STRING COMMENT'业绩激活员工所属片区',
	perf_active_emp_city STRING COMMENT'业绩激活员工所属城市',
	perf_active_emp_province STRING COMMENT'业绩激活员工所属省份',
	perf_active_emp_region STRING COMMENT'业绩激活员工所属大区',
	perf_active_emp_department STRING COMMENT'业绩激活员工所属部门', 
	perf_active_emp_belong INT COMMENT'业绩激活员工所属(1HCB内部员工/2HCB代理员工/3运满满员工)',
	perf_active_emp_perf_belong STRING COMMENT'业绩激活来源(三大区/呼叫中心/运满满/HCB代理/自主激活)'
)COMMENT '集团所有卡业绩激活数据' partitioned by (dt string) stored as orc;
 
---计算卡激活业绩所属
---1、A员工操作激活了,算A员工,即使B在24小时内卖了 套餐也算A的
---2、司机当日自主操作激活（首冲），关联给24小时内第一个销售卖套餐（指定套餐，套餐维护统一邮件发送到通知到辜娟）的员工；
---3、司机自己操作激活当日没有销售套餐员工，就单独归为一类自主激活；
---版本二：（20190318林久益新增，仅针对业绩激活卡，不影响套餐激活）
---3、司机自己操作激活当日没有销售套餐员工，查询开卡时间与激活时间相差十天以上（或为非平台卡），且不满足第4条，就单独归为一类自主激活；
---4、司机自己操作激活当日没有销售套餐员工，查询开卡时间与激活时间相差十天以内，就计算开卡员工/代理的激活业绩；

ALTER TABLE dws.etc_core_performance_acitve_s_d DROP IF EXISTS PARTITION(dt='${datekey}'); 
insert overwrite table dws.etc_core_performance_acitve_s_d partition(dt= '${datekey}')
select x.etc_card_no
	  ,x.card_name
	  ,x.info.active_amount
	  ,case when deposit_employ.proxy_user_id is not null then 1
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then 2
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then 1
	  		else 3
	   end perf_active_type
	  ,case when (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo.info.order_no end perf_active_combo_ord
	  ,case when (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo.info.combo_code end perf_active_combo_code
	  ,case when (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo.info.combo_name end perf_active_combo_name
	  ,case when deposit_employ.proxy_user_id is not null then x.info.active_time 
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo.info.sell_time
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then x.info.active_time
	   end perf_active_time

	  ,case when deposit_employ.proxy_user_id is not null then deposit_employ.proxy_user_id 
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo_employ.proxy_user_id
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then open_employ.proxy_user_id
	   end perf_active_emp_user_id 
	  ,case when deposit_employ.proxy_user_id is not null then deposit_employ.emp_name 
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo_employ.emp_name
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then open_employ.emp_name
	   end	perf_active_emp_name
	  ,case when deposit_employ.proxy_user_id is not null then deposit_employ.emp_work_no 
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo_employ.emp_work_no
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then open_employ.emp_work_no
	   end  perf_active_emp_work_no
	  ,case when deposit_employ.proxy_user_id is not null then deposit_employ.proxy_ymm_work_no 
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo_employ.proxy_ymm_work_no
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then open_employ.proxy_ymm_work_no
	   end	perf_active_emp_ymm_work_no
	  ,case when deposit_employ.proxy_user_id is not null then deposit_employ.emp_area 
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo_employ.emp_area
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then open_employ.emp_area
	   end perf_active_emp_area
	  ,case when deposit_employ.proxy_user_id is not null then deposit_employ.emp_city 
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo_employ.emp_city
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then open_employ.emp_city
	   end	perf_active_emp_city
	  ,case when deposit_employ.proxy_user_id is not null then deposit_employ.emp_province 
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo_employ.emp_province
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then open_employ.emp_province
	   end	perf_active_emp_province
	  ,case when deposit_employ.proxy_user_id is not null then deposit_employ.emp_region 
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo_employ.emp_region
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then open_employ.emp_region
	   end	perf_active_emp_region
	  ,case when deposit_employ.proxy_user_id is not null then deposit_employ.emp_department 
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo_employ.emp_department
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then open_employ.emp_department
	   end	perf_active_emp_department
	  ,case when deposit_employ.proxy_user_id is not null then deposit_employ.emp_belong 
	  		when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  			 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo_employ.emp_belong
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then open_employ.emp_belong
	   end	perf_active_emp_belong
	  ,case when deposit_employ.proxy_user_id is not null then deposit_employ.emp_perf_belong
	  	    when deposit_employ.proxy_user_id is null and combo.etc_card is not null 
	  	    	 and (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 between 0 and 24 then combo.info.emp_perf_belong
			when deposit_employ.proxy_user_id is null and (combo.etc_card is null
				 or (unix_timestamp(combo.info.sell_time) - unix_timestamp(x.info.active_time))/3600 > 24)
				 and datediff(x.info.active_time,open.open_time)<=10 then open_employ.emp_perf_belong
	  		else '自主激活'
	   end perf_active_emp_perf_belong
from (
	select deposit.etc_card_no
		  ,min(named_struct('active_time',deposit_time,'operate_user_id',operator_id,'active_amount',order_amount)) info 
	      ,card.product_name as card_name
	from dwd.etc_deposit_order_fact_s_d deposit 
	left join  dim.etc_card_dim_s_d card on deposit.etc_card_no=card.etc_card_no and card.dt='${datekey}'
	where deposit.dt = '${datekey}'
	and deposit.origin_status = 3
	and to_date(deposit.deposit_time) <= '${datekey}'
	group by deposit.etc_card_no,product_name
	)x 
left join
(select * from dwd.etc_open_order_fact_s_d where dt='${datekey}' and status=1) open
on x.etc_card_no=open.etc_card_no
left join (
	SELECT *
	from dwd.etc_employee_dim_s_d
	where dt = '${datekey}'
	)deposit_employ on x.info.operate_user_id = deposit_employ.proxy_user_id
left join (
	SELECT *
	from dwd.etc_employee_dim_s_d 
	where dt = '${datekey}'
	)open_employ on open.open_user_id = open_employ.proxy_user_id
left join (
	select combo_etc.etc_card
		  ,min(named_struct('sell_time',combo.sell_time,'proxy_user_id',employ.proxy_user_id,'emp_perf_belong',employ.emp_perf_belong,
		  				    'combo_name',combo_list.combo_name,'combo_code',combo_list.combo_code,'order_no',combo.order_no)) info 
	from (
	  select buyer_id
	      ,creator_id employ_user_id
	      ,create_time sell_time
	      ,combo_code
	      ,order_no
	      ,buyer_mobile
	    from ods.etc_h088_combo_order
	    where dt = '${datekey}'
	      and status = 1
	      and to_date(create_time) <= '${datekey}'
	   )combo
	join dw_etc.dwd_etc_combo_list combo_list on combo.combo_code = combo_list.combo_code and combo_list.is_perf = 1 
	join (
		SELECT *
		from dwd.etc_employee_dim_s_d 
		where dt = '${datekey}'
		)employ on combo.employ_user_id = employ.proxy_user_id
	JOIN (
	    SELECT order_no,etc_card
	    from dw_etc.dwd_etc_card_combo_info    -----
	    where dt = '${datekey}'
	    ) combo_etc on combo.order_no = combo_etc.order_no
	group by combo_etc.etc_card
	)combo on combo.etc_card = x.etc_card_no 
left join (
		SELECT *
		from dwd.etc_employee_dim_s_d 
		where dt = '${datekey}'
		) combo_employ on combo.info.proxy_user_id = combo_employ.proxy_user_id;