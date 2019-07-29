set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=32;
set mapred.reduce.tasks = 50;

create table if not exists dws.etc_core_active_info_s_d(
etc_card_no STRING COMMENT '卡号',
active_province STRING COMMENT '激活省份',
active_time STRING COMMENT '激活时间',
active_order_id STRING COMMENT '卡激活时充值订单号',
active_amount DECIMAL(12,2) COMMENT '激活首充金额',
active_operate_user_id STRING COMMENT '激活首充充值人user_id',

active_app_channel INT COMMENT '激活app区分0管理版/1司机版/2企业版',
active_channel INT COMMENT '激活渠道1 2-货车帮/3运满满',
active_emp_name STRING COMMENT '激活操作员工姓名',
active_emp_work_no STRING COMMENT '激活操作员工工号',
active_emp_ymm_work_no STRING COMMENT '激活操作员工YMM工号',
active_emp_area STRING COMMENT '激活操作员工所属片区',
active_emp_city STRING COMMENT '激活操作员工所属城市',
active_emp_province STRING COMMENT '激活操作员工所属省份',
active_emp_region STRING COMMENT '激活操作员工所属大区',
active_emp_department STRING COMMENT '激活操作员工所属部门所属渠道', 
active_emp_belong INT COMMENT '激活操作员工所属(1HCB内部员工/2HCB代理员工/3运满满员工)',
active_emp_perf_belong STRING COMMENT '激活操作员工业绩来源(三大区/呼叫中心/运满满/HCB代理)',
active_org_id STRING COMMENT '激活所属org_id',

active_deposit_cnt_30 DECIMAL(12,2) COMMENT '激活后30天内充值次数(含激活)',
active_deposit_amt_30 DECIMAL(12,2) COMMENT '激活后30天内充值金额(含激活)',
last_deposit_time STRING COMMENT '最后充值时间',
last_deposit_amount DECIMAL(12,2) COMMENT '最后充值金额',
last_duration INT COMMENT 'ETC最后一笔充值距今时长',
last_cash_duration INT COMMENT 'ETC最后一笔现金充值距今时长',
last_baitiao_duration INT COMMENT 'ETC最后一笔白条充值距今时长',
baitiao_active_time STRING COMMENT '白条激活时间',
total_deposit_cnt INT COMMENT '累计充值次数',
total_deposit_amt DECIMAL(12,2) COMMENT '累计充值金额',
total_company_cnt DECIMAL(12,2) COMMENT '企业累计充值次数',
total_company_amt DECIMAL(12,2) COMMENT '企业累计充值金额',
repurchase_cnt_day INT COMMENT '当日复购次数',
repurchase_amt_day DECIMAL(12,2) COMMENT '当日复购金额',
repurchase_cnt_week INT COMMENT '近7天复购次数',
repurchase_amt_week DECIMAL(12,2) COMMENT '近7天复购金额',
repurchase_cnt_30 INT COMMENT '近30天复购次数',
repurchase_amt_30 DECIMAL(12,2) COMMENT '近30天复购金额',
repurchase_amt_total DECIMAL(12,2) COMMENT '历史累计复购金额',
repurchase_amt_last DECIMAL(12,2) COMMENT '最后复购金额',
repurchase_last_time STRING COMMENT '最后复购时间',
silence_cycle INT COMMENT '沉默周期(现在时间-最后复购时间)',
miss_time STRING COMMENT '最近流失时间',
recall_time STRING COMMENT '最近60天流失回归时间',
recall_interval INT COMMENT '最近60天流失回归间隔天数',
recall_deposit_day DECIMAL(12,2) COMMENT '最近60天流失回归当天充值金额',
recall_deposit_week DECIMAL(12,2) COMMENT '最近60天流失回归7天充值金额',
recall_deposit_30 DECIMAL(12,2) COMMENT '最近60天流失回归30天充值金额',
before_amt_30 DECIMAL(12,2) COMMENT '最后充值前30天累计充值金额',
before_cnt_30 INT COMMENT '最后充值前30天累计充值次数',
before_avg_amt_30 DECIMAL(12,2) COMMENT '最后充值前30天次均充值金额',
before_amt_60 DECIMAL(12,2) COMMENT '最后充值前60天累计充值金额',
before_cnt_60 INT COMMENT '最后充值前60天累计充值次数',
before_avg_amt_60 DECIMAL(12,2) COMMENT '最后充值前60天次均充值金额',
before_max_amt_30 DECIMAL(12,2) COMMENT '距最后充值时间30天内单次充值最高金额',
before_max_amt_60 DECIMAL(12,2) COMMENT '距最后充值时间60天内单次充值最高金额',
recent_deposit_freq_60 DECIMAL(12,2) COMMENT '60天充值频率(60/近60天内充值次数)',
recent_deposit_freq_30 DECIMAL(12,2) COMMENT '30天充值频率(30/近30天内充值次数)',
recent_deposit_amt_30 DECIMAL(12,2) COMMENT '近30天累计充值金额',
recent_deposit_cnt_30 INT COMMENT '近30天累计充值次数',
recent_deposit_avg_amt_30 DECIMAL(12,2) COMMENT '近30天次均充值金额',
recent_deposit_amt_60 DECIMAL(12,2) COMMENT '近60天累计充值金额',
recent_deposit_cnt_60 INT COMMENT '近60天累计充值次数',
recent_deposit_avg_amt_60 DECIMAL(12,2) COMMENT '近60天次均充值金额',
last_operate_id string comment '最后充值人id'
)COMMENT '集团所有卡激活数据' partitioned by (dt string) stored as orc;




insert overwrite table dws.etc_core_active_info_s_d partition(dt='${datekey}')
	select etc_card_no
	      ,regexp_extract(regexp_replace(active_info.deposit_address,'中国',''),'(^.+?)(省|市|壮族自治区|回族自治区|维吾尔自治区|自治区|特别行政区)',1) active_province
		  ,date_format(active_info.deposit_time,'yyyy-MM-dd HH:mm:ss') active_time
		  ,active_info.active_order_id active_order_id
		  ,active_info.deposit_amount active_amount
		  ,active_info.operator_user_id  as active_operate_user_id

		  ,active_info.active_app_channel
		  ,active_info.active_channel
		  ,active_info.active_emp_name
		  ,active_info.active_emp_work_no
		  ,active_info.active_emp_ymm_work_no
		  ,active_info.active_emp_area
		  ,active_info.active_emp_city
		  ,active_info.active_emp_province
		  ,active_info.active_emp_region
		  ,active_info.active_emp_department
		  ,active_info.active_emp_belong
		  ,active_info.active_emp_perf_belong
		  ,active_info.active_org_id

		  ,active_deposit_cnt_30
		  ,active_deposit_amt_30
		  ,date_format(last_deposit_time,'yyyy-MM-dd HH:mm:ss') last_deposit_time
		  ,last_deposit_amount
		  ,last_duration
		  ,last_cash_duration
		  ,last_baitiao_duration
		  ,date_format(baitiao_active_time,'yyyy-MM-dd HH:mm:ss') baitiao_active_time
		  ,total_deposit_cnt
		  ,total_deposit_amt
		  ,total_company_amt
		  ,total_company_cnt
		  ,repurchase_cnt_day
		  ,repurchase_amt_day
		  ,repurchase_cnt_week
		  ,repurchase_amt_week
		  ,repurchase_cnt_30
		  ,repurchase_amt_30
		  ,repurchase_amt_total
		  ,repurchase_info.deposit_amount repurchase_amt_last
		  ,date_format(repurchase_info.deposit_time,'yyyy-MM-dd HH:mm:ss') repurchase_last_time
		  ,datediff('${datekey}',repurchase_info.deposit_time) silence_cycle
		  ,date_format(miss_time,'yyyy-MM-dd HH:mm:ss') miss_time
		  ,date_format(recall_time,'yyyy-MM-dd HH:mm:ss') recall_time
		  ,recall_interval
		  ,recall_deposit_day
		  ,recall_deposit_week
		  ,recall_deposit_30
		  ,before_amt_30
		  ,before_cnt_30
		  ,before_avg_amt_30
		  ,before_amt_60
		  ,before_cnt_60
		  ,before_avg_amt_60
		  ,before_max_amt_30
		  ,before_max_amt_60
		  ,recent_deposit_freq_60
		  ,recent_deposit_freq_30
		  ,recent_deposit_amt_30
		  ,recent_deposit_cnt_30
		  ,recent_deposit_avg_amt_30
		  ,recent_deposit_amt_60
		  ,recent_deposit_cnt_60
		  ,recent_deposit_avg_amt_60
		  ,last_operate_id
	FROM (
		select sum(case when to_date(y.deposit_time) between date_sub(x.last_info.deposit_time,29) and to_date(x.last_info.deposit_time) then y.deposit_amount end) before_amt_30
			  ,count(case when to_date(y.deposit_time) between date_sub(x.last_info.deposit_time,29) and to_date(x.last_info.deposit_time) then 1 end) before_cnt_30
		      ,round(sum(case when to_date(y.deposit_time) between date_sub(x.last_info.deposit_time,29) and to_date(x.last_info.deposit_time) then y.deposit_amount end)/count(case when to_date(y.deposit_time) between date_sub(x.last_info.deposit_time,29) and to_date(x.last_info.deposit_time) then 1 end),2) before_avg_amt_30
		      ,sum(case when to_date(y.deposit_time) between date_sub(x.last_info.deposit_time,59) and to_date(x.last_info.deposit_time) then y.deposit_amount end) before_amt_60
		      ,count(case when to_date(y.deposit_time) between date_sub(x.last_info.deposit_time,59) and to_date(x.last_info.deposit_time) then 1 end) before_cnt_60
		      ,round(sum(case when to_date(y.deposit_time) between date_sub(x.last_info.deposit_time,59) and to_date(x.last_info.deposit_time) then y.deposit_amount end)/count(case when to_date(y.deposit_time) between date_sub(x.last_info.deposit_time,59) and to_date(x.last_info.deposit_time) then 1 end),2) before_avg_amt_60
		      ,max(case when to_date(y.deposit_time) between date_sub(x.last_info.deposit_time,29) and to_date(x.last_info.deposit_time) then y.deposit_amount end) before_max_amt_30
		      ,max(case when to_date(y.deposit_time) between date_sub(x.last_info.deposit_time,59) and to_date(x.last_info.deposit_time) then y.deposit_amount end) before_max_amt_60
		      ,sum(case when to_date(y.deposit_time) between date_sub('${datekey}',29) and '${datekey}' then y.deposit_amount end) recent_deposit_amt_30
			  ,count(case when to_date(y.deposit_time) between date_sub('${datekey}',29) and '${datekey}' then 1 end) recent_deposit_cnt_30
			  ,round(30/count(case when to_date(y.deposit_time) between date_sub('${datekey}',29) and '${datekey}' then 1 end),2) recent_deposit_freq_30
		      ,round(sum(case when to_date(y.deposit_time) between date_sub('${datekey}',29) and '${datekey}' then y.deposit_amount end)/count(case when to_date(y.deposit_time) between date_sub('${datekey}',29) and '${datekey}' then 1 end),2) recent_deposit_avg_amt_30
		      ,sum(case when to_date(y.deposit_time) between date_sub('${datekey}',59) and '${datekey}' then y.deposit_amount end) recent_deposit_amt_60
		      ,count(case when to_date(y.deposit_time) between date_sub('${datekey}',59) and '${datekey}' then 1 end) recent_deposit_cnt_60
		      ,round(60/count(case when to_date(y.deposit_time) between date_sub('${datekey}',59) and '${datekey}' then 1 end),2) recent_deposit_freq_60
		      ,round(sum(case when to_date(y.deposit_time) between date_sub('${datekey}',59) and '${datekey}' then y.deposit_amount end)/count(case when to_date(y.deposit_time) between date_sub('${datekey}',59) and '${datekey}' then 1 end),2) recent_deposit_avg_amt_60
		      ,sum(y.deposit_amount) total_deposit_amt
			  ,sum(case when y.deposit_type=1 then y.deposit_amount else 0 end) as  total_company_amt
			  ,count(1) total_deposit_cnt
			  ,count(case when y.deposit_type=1 then 1 else null end) as  total_company_cnt
			  ,sum(case when datediff(y.deposit_time,active_info.deposit_time) <= 30 then 1 end) active_deposit_cnt_30
		      ,sum(case when datediff(y.deposit_time,active_info.deposit_time) <= 30 then y.deposit_amount end) active_deposit_amt_30


			  ,sum(case when to_date(y.deposit_time) = to_date(recall_info.next_deposit_time) then y.deposit_amount end) recall_deposit_day
			  ,sum(case when to_date(y.deposit_time) between to_date(recall_info.next_deposit_time) and date_add(recall_info.next_deposit_time,6) then y.deposit_amount end) recall_deposit_week
			  ,sum(case when to_date(y.deposit_time) between to_date(recall_info.next_deposit_time) and date_add(recall_info.next_deposit_time,29) then y.deposit_amount end) recall_deposit_30
			  
			  ,date_add(recall_info.deposit_time,60) miss_time
			  ,recall_info.next_deposit_time recall_time
			  ,recall_info.days_interval recall_interval
		      ,x.etc_card_no
		      ,x.last_cash_duration
		      ,x.last_baitiao_duration
		      ,x.baitiao_active_time
		      ,x.last_duration
		      ,x.last_info.deposit_time last_deposit_time
			  ,x.last_info.deposit_amount last_deposit_amount
			  ,x.last_info.operator_user_id as last_operate_id
		      ,repurchase_cnt_day
			  ,repurchase_amt_day
			  ,repurchase_cnt_week
			  ,repurchase_amt_week
			  ,repurchase_cnt_30
			  ,repurchase_amt_30
			  ,repurchase_amt_total
			  ,repurchase_cycle_60
			  ,repurchase_info
			  ,active_info
		from (

		    select deposit.etc_card_no
			      ,datediff('${datekey}',max(case when white_bar.outer_order_id is null then deposit.deposit_time end)) last_cash_duration
			      ,datediff('${datekey}',max(case when white_bar.outer_order_id is not null then deposit.deposit_time end)) last_baitiao_duration
			      ,min(case when white_bar.outer_order_id is not null then deposit.deposit_time end) baitiao_active_time
			      ,datediff('${datekey}',max(deposit.deposit_time)) last_duration
			      ,min(named_struct('deposit_time',deposit_time,'deposit_amount',deposit_amount,'deposit_address',deposit_address,'active_order_id',order_id
			      				   ,'operator_user_id',operator_user_id,'active_app_channel',app_channel,'active_channel',channel,'active_emp_name',employ.emp_name
			      				   ,'active_emp_work_no',employ.emp_work_no,'active_emp_ymm_work_no',employ.proxy_ymm_work_no
			      				   ,'active_emp_area',employ.emp_area,'active_emp_city',employ.emp_city,'active_emp_province',employ.emp_province
			      				   ,'active_emp_region',employ.emp_region,'active_emp_department',employ.emp_department
			      				   ,'active_emp_belong',employ.emp_belong,'active_emp_perf_belong',employ.emp_perf_belong
			      				   ,'active_org_id',company_etc.org_id)) active_info
		    	  ,max(named_struct('deposit_time',deposit_time,'deposit_amount',deposit_amount,'operator_user_id',operator_user_id)) as last_info
		    	  ,count(case when to_date(deposit_time) = '${datekey}' and deposit.rnum > 1 then 1 end) repurchase_cnt_day
				  ,sum(case when to_date(deposit_time) = '${datekey}' and deposit.rnum > 1 then deposit_amount end) repurchase_amt_day
				  ,count(case when datediff('${datekey}',deposit_time) between 0 and 6 and deposit.rnum > 1 then 1 end) repurchase_cnt_week
				  ,sum(case when datediff('${datekey}',deposit_time) between 0 and 6 and deposit.rnum > 1 then deposit_amount end) repurchase_amt_week
				  ,count(case when datediff('${datekey}',deposit_time) between 0 and 29 and deposit.rnum > 1 then 1 end) repurchase_cnt_30
				  ,sum(case when datediff('${datekey}',deposit_time) between 0 and 29 and deposit.rnum > 1 then deposit_amount end) repurchase_amt_30
				  ,sum(case when to_date(deposit_time) <= '${datekey}' and deposit.rnum > 1 then deposit_amount end) repurchase_amt_total
				  ,round(60/count(case when datediff('${datekey}',deposit_time) between 0 and 59 and deposit.rnum > 1 then 1 end),2) repurchase_cycle_60
				  ,max(named_struct('deposit_time',case when deposit.rnum > 1 then deposit_time end,'deposit_amount',case when deposit.rnum > 1 then deposit_amount end)) repurchase_info

				  ,max(named_struct('deposit_time',case when deposit.days_interval >= 60 then deposit_time
														when deposit.next_deposit_time is null and datediff('${datekey}',deposit.deposit_time)>=60 then deposit_time end,
									'deposit_amount',deposit_amount,'days_interval',days_interval,'next_deposit_time',next_deposit_time)) recall_info
			from(
				select deposit_time
					  ,order_amount as deposit_amount
					  ,etc_card_no
					  ,order_id
					  ,deposit_address
					  ,operator_id as operator_user_id
					  ,order_channel as app_channel-- INT COMMENT '充值app区分0管理版/1司机版/2企业版',
					  ,channel --INT COMMENT '充值渠道1 2-货车帮/3运满满',
					  ,row_number() over (PARTITION BY etc_card_no order by deposit_time asc) as rnum 
					  ,datediff(lead(deposit_time) over(partition by etc_card_no order by deposit_time),deposit_time) days_interval
	      			  ,lead(deposit_time) over(partition by etc_card_no order by deposit_time) next_deposit_time
				from  dwd.etc_deposit_order_fact_s_d a
				where dt = '${datekey}'
					and  origin_status = 3
					and to_date(deposit_time) <= '${datekey}'
				)deposit
			left join (
				SELECT *
				from  dwd.etc_employee_dim_s_d
				WHERE dt = '${datekey}'
				)employ on deposit.operator_user_id = employ.proxy_user_id
			left join (
					select order_no,max(org_id) org_id    ---为了避免企业充值表出错
					from dw_etc.dw_etc_company_deposit_order_new
					where dt = '${datekey}'
					group by order_no
					)company_etc on deposit.order_id = company_etc.order_no
			left join (
				select outer_order_id
				--白条借款表
				from ods.loan_h155_ls_loan_project_order
				where dt = '${datekey}'
					and status = '20_SUCCESS'
					and loan_type = 'WITHE_BAR'
				group by outer_order_id
				union 
				select outer_order_id
				--白条借款表
				from ods.loan_h155_ls_loan_project_order
				where dt = '${datekey}'
					and status not in ('99_TEST')
				group by outer_order_id
				) white_bar on deposit.order_id = white_bar.outer_order_id
			group by deposit.etc_card_no

			) x 
		left join(
		    select etc_card_no
		    	  ,order_amount deposit_amount
		    	  ,deposit_time
		    	  ,order_id
				  ,deposit_type
		    from dwd.etc_deposit_order_fact_s_d
		    where dt = '${datekey}'
		    	and origin_status = 3
		    	and to_date(deposit_time) <= '${datekey}'
			)y on x.etc_card_no= y.etc_card_no

			group by x.etc_card_no
			      ,x.last_cash_duration
			      ,x.last_baitiao_duration
			      ,x.baitiao_active_time
			      ,x.last_duration
			      ,x.last_info.deposit_time
				  ,x.last_info.deposit_amount 
				  ,x.last_info.operator_user_id
			      ,repurchase_cnt_day
				  ,repurchase_amt_day
				  ,repurchase_cnt_week
				  ,repurchase_amt_week
				  ,repurchase_cnt_30
				  ,repurchase_amt_30
				  ,repurchase_amt_total
				  ,repurchase_cycle_60
				  ,repurchase_info
				  ,recall_info.deposit_time 
				  ,recall_info.next_deposit_time 
				  ,recall_info.days_interval
				  ,active_info
		)finance;