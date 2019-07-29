CREATE TABLE if not exists dmt.etc_card_s_d( 

---基本信息
etc_card_no  			string 	     comment 'ETC卡号',
card_name    			string 	     comment 'ETC卡名',
card_type    			string 	     comment 'ETC卡类型(储值卡/记账卡)',
van_number   			string 	     comment '车牌号',
van_plate_color 		string 	     comment '车牌颜色',
user_id      			bigint 	     comment '司机用户id',
truck_user_type 		string    	  comment '司机主副驾类型',
realname     			string 	     comment '司机姓名',
mobile       			string 	     comment '司机电话号码',
ic_no        			string 	     comment '司机身份证号码',
age          			int 	     comment '司机年龄',
register_time  			string 	     comment '司机注册时间',
register_city  			string       comment '司机注册城市',
register_prov  			string       comment '司机注册省份',
baitiao_apply_time      string       comment '白条首次申请时间',
baitiao_credit_status    string        comment '白条授信状态',
baitiao_credit_success_time string    comment '白条首次授信成功时间',
last_30d_login_days      int          comment '最近30天活跃天数',


----开卡
card_user_name		string	comment	'姓名',
open_phone 			string  comment '开卡人预留手机号',
open_ic_type		string	comment	'开卡证件类型:1-身份证/2-军官证/3-其他',
open_ic_no			string	comment	'开卡证件号',
open_type			int		comment	'开户类型:0-个人/1-单位',
open_platform 		string  comment '开卡平台(HCB/YMM/NONE)',
open_time			string	comment	'开卡时间',
open_emp_user_id	 string	comment	'开卡员工id',
open_emp_user_type	string comment '开卡员工类型 (hcb员工/hcb代理/ymm员工)',
open_emp_work_no  	string comment '开卡员工工号',
open_emp_name     	string comment '开卡员工姓名',
open_emp_region   	string	COMMENT	'开卡员工所属大区',
open_emp_area 	  	string	COMMENT	'开卡员工所属片区',
open_emp_city 				string	COMMENT	'开卡员工所属城市',
open_emp_province 			string	COMMENT	'开卡员工所属省份',
company_org_id  	string  comment '企业org_id', 
bind_company_status string comment '绑定企业状态',
bind_company_list 	string comment '绑定企业账号',
is_company_open 	string comment '是否企业统一开卡',

-----销卡
cancel_type				string		comment	'销卡类型:补卡/销卡/换卡',
cancel_time			string	comment	'销卡时间',
us_cancel_id			bigint		comment '销卡员工ID',
us_cancel_type			string		comment '销卡员工类型 (hcb员工/hcb代理/ymm员工)',
us_cancel_workno			string		comment '销卡员工工号',
us_cancel_name			string		comment '销卡员工姓名',
cancel_emp_region   	string	COMMENT	'销卡员工所属大区',
calcel_emp_area 	  string	COMMENT	'销卡员工所属片区',
calcel_emp_city 			string	COMMENT	'销卡员工所属城市',
calcel_emp_province 		string	COMMENT	'销卡员工所属省份',
cancel_reason_name			string	comment	'销卡原因',
cancel_status			string 	comment	'销卡订单状态',


----充值
active_time       			string comment '激活时间',
active_province      		string comment '激活省份',
active_order_id      		string comment '激活充值订单号',
active_amount   			double comment '激活充值金额',
active_operate_user_id  	 bigint comment '激活充值人user_id',
active_deposit_cnt_30		double comment '激活后30天内充值次数(含激活)',
active_deposit_amt_30		double comment '激活后30天内充值金额(含激活)',
last_deposit_time  			string comment '最后充值时间',
last_deposit_user_id  		bigint comment '最后充值人user_id',
last_deposit_amount 		 double comment '最后充值金额',
last_duration_days 			bigint comment '最后一笔充值距今天数',
last_cash_duration_days 	bigint comment '最后一笔现金充值距今天数',
last_baitiao_duration_days	bigint comment '最后一笔白条充值距今时天数',

baitiao_active_time			string	comment '白条激活时间',
ac_deposit_amount 			double comment '累计充值金额',
ac_deposit_times 			bigint comment '累计充值次数',
org_ac_deposit_amount 		double comment '企业累计充值金额',
org_ac_deposit_times 		bigint comment '企业累计充值次数',
last_7d_deposit_amount 		double comment '近7天充值金额',
last_7d_deposit_times 		double comment '近7天充值次数',
last_30d_deposit_amount 	double comment '近30天充值金额',
last_30d_deposit_times 		double comment '近30天充值次数',
last_60d_deposit_amount 	double comment '近60天充值金额',
last_60d_deposit_times 		double comment '近60天充值次数',

----消费
first_consum_time 	string comment '首次消费时间',
first_consum_amount double comment '首次消费金额',
last_consum_time 	string comment '最后消费时间',
last_consum_amount 	string comment '最后消费金额',

last_7d_consum_amount  double comment '近7天累计消费金额',
last_30d_consum_amount  double comment '近30天累计消费金额',
last_60d_consum_amount  double comment '近60天累计消费金额',


-----业绩
perf_active_type		string comment '业绩激活类型',
perf_active_combo_ord	string	comment '业绩激活时套餐订单号',
perf_active_combo_code	string	comment '业绩激活时套餐编码',
perf_active_combo_name	string	comment '业绩激活时套餐名',
perf_active_time		string	comment '业绩激活时间',
perf_active_emp_user_id	string	comment '业绩激活员工user_id',
perf_active_emp_name	string	comment '业绩激活员工名字',
perf_active_emp_work_no	string	comment '业绩激活员工工号',
perf_active_emp_ymm_work_no	string	comment '业绩激活员工YMM工号',
perf_active_emp_area	string	comment '业绩激活员工所属片区',
perf_active_emp_city	string	comment '业绩激活员工所属城市',
perf_active_emp_province	string	comment '业绩激活员工所属省份',
perf_active_emp_region		string	comment '业绩激活员工所属大区',
perf_active_emp_department	string	comment '业绩激活员工所属部门',
perf_active_emp_belong	string  comment '业绩激活员工类型(1HCB内部员工/2HCB代理员工/3运满满员工)',

miss_time	string	comment '最近流失时间',
recall_time	string	comment '最近60天流失回归时间',

-----车货匹配
first_province			string	comment 'top1常跑省份(最近60天)',
first_province_days		string	comment 'top1常跑省份天数(最近60天)',
first_dis_province		string	comment 'top1打折省份(最近60天)',
first_dis_province_days	string	comment 'top1打折省份天数(最近60天)',
second_dis_province		string	comment 'top2打折省份(最近60天)',
third_dis_province		string	comment 'top3打折省份(最近60天)'
)comment'ETC卡大宽表'
partitioned by (dt string) stored as orc;



ALTER TABLE dmt.etc_card_s_d DROP IF EXISTS PARTITION(dt='${datekey}'); 
insert overwrite table dmt.etc_card_s_d partition(dt= '${datekey}')
select card.etc_card_no  			
       ,product_name card_name    			
       ,card_type    			
       ,van_number   			
       ,van_plate_color 		
       ,card.user_id      			
       ,truck_user_type 		
       ,realname     			
       ,unit_mobile as mobile       			
       ,ic_no        			
       ,age          			
       ,driver.register_time  			
       ,driver.city_name register_city
	   ,driver.province_name  as register_prov 			
       ,baitiao_apply_time      
       ,baitiao_credit_statu    
       ,baitiao_credit_success_time
       ,last_30d_login_days     
	   
	   ,card_user_name		
	   ,open_phone 			
	   ,open_ic_type		
	   ,open_ic_no			
	   ,open_type			
	   ,open_platform 		
	   ,open_time			
	   ,open_emp_user_id	
	   ,open_emp_user_type	
	   ,open_emp_work_no  	
	   ,open_emp_name     	
	   ,open_emp_region   	
	   ,open_emp_area 	  	
	   ,open_emp_city 		
	   ,open_emp_province 	
	   ,open_card.company_org_id  	
	   ,bind_company_status 
	   ,bind_company_list 	
	   ,is_company_open 	
	   
	   ,cancel_type			
	   ,cancel_time			
	   ,us_cancel_id		
	   ,us_cancel_type		
	   ,us_cancel_workno	
	   ,us_cancel_name		
	   ,cancel_emp_region   
	   ,cancel_emp_area 	
	   ,cancel_emp_city 	
	   ,cancel_emp_province 
	   ,cancel_reason_name	
	   ,cancel_status		
	   
	   ,active_time       			
	   ,active_province      		
	   ,active_order_id      		
	   ,active_amount   			
	   ,active_operate_user_id  	
	   ,active_deposit_cnt_30		
	   ,active_deposit_amt_30		
	   ,card.last_deposit_time  			
	   ,last_deposit_user_id  		
	   ,last_deposit_amount 		
	   ,last_duration_days 			
	   ,last_cash_duration_days 	
	   ,last_baitiao_duration_days
	   ,baitiao_active_time
	   
	   ,ac_deposit_amount 			
	   ,ac_deposit_times 			
	   ,org_ac_deposit_amount 		
	   ,org_ac_deposit_times 		
	   ,last_7d_deposit_amount 		
	   ,last_7d_deposit_times 		
	   ,last_30d_deposit_amount 	
	   ,last_30d_deposit_times 		
	   ,last_60d_deposit_amount 	
	   ,last_60d_deposit_times 		
	   
	  ,active_info.trade_time as first_consum_time 
	  ,active_info.trade_money as  first_consum_amount 
	  ,last_info.trade_time as last_consum_time 
	  ,last_info.trade_money as  last_consum_amount 
	  
	  ,last_7d_consum_amount 
	  ,last_30d_consum_amount
	  ,last_60d_consum_amount
	   
	  ,perf_active_type		
	  ,perf_active_combo_ord	
	  ,perf_active_combo_code	
	  ,perf_active_combo_name	
	  ,perf_active_time		
	  ,perf_active_emp_user_id	
	  ,perf_active_emp_name	
	  ,perf_active_emp_work_no	
	  ,perf_active_emp_ymm_work_no
	  ,perf_active_emp_area	
	  ,perf_active_emp_city	
	  ,perf_active_emp_province	
	  ,perf_active_emp_region		
	  ,perf_active_emp_department
	  ,perf_active_emp_belong
	  ,miss_time	
      ,recall_time	
	  ,first_province			
	  ,first_province_days		
	  ,first_dis_province		
	  ,first_dis_province_days	
	  ,second_dis_province		
	  ,third_dis_province		

from(
	select *
	from  dim.etc_card_dim_s_d a
	where dt='${datekey}'
	)card 
left join(
	select user_id,
			truck_user_type ,
			realname,
			unit_mobile,
			ic_no,
			age,
			register_time,
			city_name,
			province_name
	from dwb.umd_user_base_info_s_d
	where dt='${datekey}'
)driver on card.user_id=driver.user_id
left join(
	select split(uid,'_')[1] as user_id,
			gmt_asset_apply_time as baitiao_apply_time,
			status as baitiao_credit_statu,
			credit_success_time as baitiao_credit_success_time			
	from ods.loan_h155_uc_user_credit
	where dt='${datekey}'
	)baitiao on card.user_id=baitiao.user_id
left join(
	select user_id,
			count(distinct dt) as last_30d_login_days
	from dwb.uce_user_session_i_d
	where dt>=date_sub('${datekey}',29)
	group by user_id
	)login on card.user_id=login.user_id
left join(
	select etc_card_no,
			card_user_name,
			open_phone,
			open_ic_type,
			open_ic_no,
			open_type,
			open_platform,
			open_time,
			open_user_id as open_emp_user_id,
			emp.emp_name as open_emp_name,
			case when emp.emp_belong=1 then 'hcb员工'
				when emp.emp_belong=2 then 'hcb代理'
				when emp.emp_belong=3 then 'ymm员工'
				end as open_emp_user_type,
			emp_work_no as open_emp_work_no,
			emp_region as open_emp_region,
			emp_area as open_emp_area,
			emp_city as open_emp_city,
			emp_province as open_emp_province,
			open.company_org_id,
				case when bind_company_status=0 then '未绑定企业'
					when bind_company_status=1 then '有营业执照企业'
					when bind_company_status=2 then '车队企业 '
					end as bind_company_status,
				bind_company_list ,	
				case when is_company_open=1 then '是'
					when is_company_open=0 then '否'
					end as is_company_open
	from dwd.etc_open_order_fact_s_d open
	left join  dwd.etc_employee_dim_s_d emp on split(open.open_user_id,'_')[1]=emp.proxy_user_id and emp.dt='${datekey}'
	where open.dt='${datekey}'
)open_card on card.etc_card_no=open_card.etc_card_no
left join(
		select etc_card_no,
				type as cancel_type,
				cancel_time,
				us_cancel_id,
				case when emp.emp_belong=1 then 'hcb员工'
					when emp.emp_belong=2 then 'hcb代理'
					when emp.emp_belong=3 then 'ymm员工'
				end as us_cancel_type,
				emp_work_no as us_cancel_workno,
				emp.emp_name as us_cancel_name,
				emp_region as cancel_emp_region,
				emp_area as cancel_emp_area,
				emp_city as cancel_emp_city,
				emp_province cancel_emp_province,
				cancel.reason_name as cancel_reason_name,
				cancel.status as cancel_status				
		from  dwd.etc_cancel_order_fact_s_d cancel
		left join dwd.etc_employee_dim_s_d emp on cancel.us_cancel_id=emp.proxy_user_id and emp.dt='${datekey}'
		where cancel.dt='${datekey}'
)cancel_card on card.etc_card_no=cancel_card.etc_card_no
left join(
	select etc_card_no,
			sum(case when to_date(deposit_time)>=date_sub('${datekey}',6) then order_amount else 0 end) as last_7d_deposit_amount,
			count(case when to_date(deposit_time)>=date_sub('${datekey}',6)  then 1 else null end) as last_7d_deposit_times,
			sum(case when to_date(deposit_time)>=date_sub('${datekey}',29) then order_amount else 0 end) as last_30d_deposit_amount,
			count(case when to_date(deposit_time)>=date_sub('${datekey}',29)  then 1 else null end) as last_30d_deposit_times,
			sum(case when to_date(deposit_time)>=date_sub('${datekey}',59) then order_amount else 0 end) as last_60d_deposit_amount,
			count(case when to_date(deposit_time)>=date_sub('${datekey}',59)  then 1 else null end) as last_60d_deposit_times
			
	from dwd.etc_deposit_order_fact_s_d
	where dt='${datekey}'
	group by etc_card_no
)deposit  on card.etc_card_no=deposit.etc_card_no
left join(
	select card_no as etc_card_no,
		min(named_struct('trade_time',trade_time,'trade_money',trade_money)) active_info,
		max(named_struct('trade_time',trade_time,'trade_money',trade_money)) last_info,
		sum(case when to_date(trade_time)>=date_sub('${datekey}',6) then trade_money else 0 end) as last_7d_consum_amount,
			sum(case when to_date(trade_time)>=date_sub('${datekey}',29) then trade_money else 0 end) as last_30d_consum_amount,
			sum(case when to_date(trade_time)>=date_sub('${datekey}',59) then trade_money else 0 end) as last_60d_consum_amount
	from ods.etc_h081_etc_pass_consume_record
	where dt='${datekey}'
	group by card_no
)consum on card.etc_card_no=consum.etc_card_no
left join(
	select etc_card_no,
			active_time,
			active_province,
			active_order_id,
			active_amount,
			active_operate_user_id,
			active_deposit_cnt_30,
			active_deposit_amt_30,
			last_deposit_time,
			last_operate_id as last_deposit_user_id,
			last_deposit_amount,
			last_duration as last_duration_days,
			last_cash_duration as last_cash_duration_days,
			last_baitiao_duration as last_baitiao_duration_days,
			total_deposit_amt as ac_deposit_amount,
			total_deposit_cnt as ac_deposit_times,
			total_company_amt as org_ac_deposit_amount,
			total_company_cnt as org_ac_deposit_times,
			baitiao_active_time,
			miss_time,
			recall_time
	from dws.etc_core_active_info_s_d 
	where dt='${datekey}'
	)active on 	card.etc_card_no=active.etc_card_no	
left join(
	select etc_card as etc_card_no,
			perf_active_type,
			perf_active_combo_ord,
			perf_active_combo_code	,
			perf_active_combo_name	,
			perf_active_time,
			perf_active_emp_user_id	,
			perf_active_emp_name,
			perf_active_emp_work_no	,
			perf_active_emp_ymm_work_no,
			perf_active_emp_area	,
			perf_active_emp_city,
			perf_active_emp_province,	
			perf_active_emp_region,		
			perf_active_emp_department,
			perf_active_emp_belong			
	from dws.etc_core_performance_acitve_s_d
	where dt='${datekey}'
	)performance on card.etc_card_no=performance.etc_card_no
left join(
	---top3 打折省份
		select x.user_id
			  ,concat_ws('|',collect_set(case when x.rowwid = 1 and is_discount=1 then province_name end)) first_dis_province
			  ,concat_ws('|',collect_set(case when x.rowwid = 1 and is_discount=1 then concat('省份:',province_name,',天数:',report_days) end)) first_dis_province_days
			  ,concat_ws('|',collect_set(case when x.rowwid = 2 and is_discount=1 then province_name end)) second_dis_province
			  ,concat_ws('|',collect_set(case when x.rowwid = 3 and is_discount=1 then province_name end)) third_dis_province
			  ,concat_ws('|',collect_set(case when x.rowwid = 1 then province_name end)) first_province
			  ,concat_ws('|',collect_set(case when x.rowwid = 1 then concat('省份:',province_name,',天数:',report_days) end)) first_province_days
			  
		from (
		    select user_id
				  ,province_name
				  ,is_discount
				  ,count(distinct dt) report_days
				  ,row_number() OVER (PARTITION BY user_id ORDER BY count(distinct dt) DESC) AS rowwid
		   	 from dw_etc.dwd_etc_user_loc_in
		    where dt between date_sub('${datekey}',59) and '${datekey}'
		   	---  and is_discount = 1
		    group by user_id
		   		    ,province_name
		   		    ,is_discount
			)x
		
		group by x.user_id 
	)loc on card.user_id= loc.user_id;