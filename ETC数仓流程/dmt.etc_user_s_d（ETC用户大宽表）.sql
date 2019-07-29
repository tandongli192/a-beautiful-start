CREATE TABLE if not exists dmt.etc_user_s_d( 
user_id   bigint comment '用户id',
ymm_uid   bigint comment 'MM侧用户id',
---realname string comment '司机姓名',
---bind_m_plate_number string comment '作为主驾绑定车牌号',
---bind_d_plate_number string comment '作为副驾绑定车牌号',

is_etc string comment '是否有平台相关ETC卡(在平台开卡或者充值)',
etc_card string comment '常用ETC卡号',
is_platform_open string comment '是否有平台开卡',
last_open_time string comment '最近一次平台开卡时间',
last_open_etc_card string comment '最近一次平台开卡ETC卡号',
last_open_card_name string comment '最近一次平台开卡ETC卡种',
last_open_plate_number string comment '最近一次平台开卡对应车牌号',
last_open_plate_color string comment '最近一次平台开卡对应车牌颜色',

first_open_time string comment '第一次平台开卡时间',
first_open_etc_card string comment '第一次平台开卡ETC卡号',
first_open_card_name string comment '第一次平台开卡ETC卡种',
first_open_plate_number string comment '第一次平台开卡对应车牌号',
first_open_plate_color string comment '第一次平台开卡对应车牌颜色',

recom_card_name string comment '推荐卡类型(最近60天)',
card_mach_ratio double  comment '卡片匹配度(top1打折省份天数/总天数)',

max_loc_prov_60d string comment '定位最多省份(近60天)',
max_loc_prov_90d string comment '定位最多省份(近90天)',
max_loc_dis_prov_60d string comment '定位最多打折省份(近60天)',
max_loc_dis_prov_90d string comment '定位最多打折省份(近90天)',
last_7d_loc_zj_days int comment '近7天在浙江省定位天数',
last_30d_loc_zj_days int comment '近30天在浙江省定位天数',
last_60d_loc_zj_days int comment '近60天在浙江省定位天数',
last_90d_loc_zj_days int comment '近90天在浙江省定位天数',

bind_cards int comment '绑定ETC卡数',
bind_cards_no string comment '绑定ETC卡号',

last_30d_etc_card string comment '近30天使用最多ETC卡号',
last_30d_card_name string comment '近30天使用最多ETC卡种',
last_60d_etc_card string comment '近60天使用最多ETC卡号',
last_60d_card_name string comment '近60天使用最多ETC卡种',

first_deposit_time string comment '首充时间',
first_deposit_order_no string comment '首充订单号',
first_deposit_etc_card string comment '首充ETC卡号',
first_deposit_amount double comment '首充充值金额',

last_deposit_time string comment '最后充值时间' ,
last_deposit_order_no string comment '最后充值订单号',
last_deposit_etc_card string comment '最后充值ETC卡号',
last_deposit_amount double comment '最后充值金额', 
last_deposit_from_days bigint comment '最后一笔充值距今天数',
 
total_deposit_amount double comment '累计充值金额',
total_deposit_times double comment '累计充值次数',
total_deposit_etc_cards double comment '累计充值ETC卡数',
last_7d_deposit_amount double comment '近7天充值金额',
last_7d_deposit_times double comment '近7天充值次数',
last_7d_deposit_etc_cards double comment '近7天充值卡数',
last_14d_deposit_amount double comment '近14天充值金额',
last_14d_deposit_times double comment '近14天充值次数',
last_14d_deposit_etc_cards double comment '近14天充值卡数',
last_30d_deposit_amount double comment '近30天充值金额',
last_30d_deposit_times double comment '近30天充值次数',
last_30d_deposit_etc_cards double comment '近30天充值卡数',
last_60d_deposit_amount double comment '近60天充值金额',
last_60d_deposit_times double comment '近60天充值次数',
last_60d_deposit_etc_cards double comment '近60天充值卡数',

user_type string comment '用户分类(沉默用户,流失用户,正常用户,其他)',
deposit_user_type string comment '用户充值分层(头部用户,活跃用户,低活用户,尝鲜用户,其他)',

app_login_7d int comment '近7天APP活跃天数',
app_login_30d int comment '近30天APP活跃天数',
app_login_60d int comment '近60天APP活跃天数',

etc_visit_7d int comment 'ETC页面7日进入天数',
etc_visit_30d int comment 'ETC页面30日进入天数',
etc_visit_60d int comment 'ETC页面60日进入天数',

is_purchase_combo string comment '是否购买充值宝'
)comment 'ETC用户宽表'
partitioned by (dt string comment 'ETL日期')
stored as orc;

----近90天定位在浙江省
drop table if exists temporarydb.temp_xuzy_etc_zj_loc;
create table if not exists temporarydb.temp_xuzy_etc_zj_loc as 
select uid,
		dt
from dwd.pub_mb_location_loc_1km_i_d
where dt between date_sub('${datekey}',89) and '${datekey}'
and province_id=28
and uid is not null
group by uid,
		dt;
		
----充值信息
drop table if exists temporarydb.temp_xuzy_etc_deposit;
create table if not exists temporarydb.temp_xuzy_etc_deposit as 
select operator_id,
		etc_card_no,
		card_name,
		count(distinct case when to_date(deposit_time) between date_sub('${datekey}',6) and '${datekey}' then order_id end) as deposit_cnt_7, 
		sum(case when to_date(deposit_time) between date_sub('${datekey}',6) and '${datekey}' then order_amount end) as deposit_amt_7,
		count(distinct case when to_date(deposit_time) between date_sub('${datekey}',13) and '${datekey}' then order_id end) as deposit_cnt_14, 
		sum(case when to_date(deposit_time) between date_sub('${datekey}',13) and '${datekey}' then order_amount end) as deposit_amt_14,
		count(distinct case when to_date(deposit_time) between date_sub('${datekey}',29) and '${datekey}' then order_id end) as deposit_cnt_30, 
		sum(case when to_date(deposit_time) between date_sub('${datekey}',29) and '${datekey}' then order_amount end) as deposit_amt_30,
		count(distinct case when to_date(deposit_time) between date_sub('${datekey}',59) and '${datekey}' then order_id end) as deposit_cnt_60, 
		sum(case when to_date(deposit_time) between date_sub('${datekey}',59) and '${datekey}' then order_amount end) as deposit_amt_60,
		count(distinct order_id) as deposit_cnt_total,
		sum(order_amount) as deposit_amt_total
from dwd.etc_deposit_order_fact_s_d 
where dt='${datekey}'
	and order_status='交易完成'
group by operator_id,
	etc_card_no,
	card_name;

alter table dmt.etc_user_s_d drop partition(dt='${datekey}');
insert into dmt.etc_user_s_d partition(dt='${datekey}')
select a.user_id,
		a.ymm_uid,
		case when b.user_id is not null then '是' else '否' end as is_etc,
		b.etc_card_no as etc_card,
		case when c.user_id is not null then '是' else '否' end as is_platform_open,
		c.last_open_info.open_time as last_open_time,
		c.last_open_info.etc_card_no as last_open_etc_card,
		c.last_open_info.card_name as last_open_card_name,
		c.last_open_info.van_number as last_open_plate_number,
		c.last_open_info.van_plate_color as last_open_plate_color,
		
		c.first_open_info.open_time as first_open_time,
		c.first_open_info.etc_card_no as first_open_etc_card,
		c.first_open_info.card_name as first_open_card_name,
		c.first_open_info.van_number as first_open_plate_number,
		c.first_open_info.van_plate_color as first_open_plate_color,
		
		loc_recom.recom_card_name,
		loc_dis60.card_mach_ratio,
		
		loc60.first_province_60 as max_loc_prov_60d,
		loc90.first_province_90 as max_loc_prov_90d,
		loc_dis60.first_dis_province_60 as max_loc_dis_prov_60d,
		loc_dis90.first_dis_province_90 as max_loc_dis_prov_90d,
		loc_zj.last_7d_zj_loc,
		loc_zj.last_30d_zj_loc,
		loc_zj.last_60d_zj_loc,
		loc_zj.last_90d_zj_loc,
		
		bind.bind_card_cnts,
		bind.bind_card_no,
		
		deposit.deposit_info_30.etc_card as last_30d_etc_card,
		deposit.deposit_info_30.card_name as last_30d_card_name,
		deposit.deposit_info_60.etc_card as last_60d_etc_card,
		deposit.deposit_info_60.card_name as last_60d_card_name,
		
		deposit_info.min_deposit_info.deposit_time as first_deposit_time,
		deposit_info.min_deposit_info.order_id as first_deposit_order_no,
		deposit_info.min_deposit_info.etc_card as first_deposit_etc_card,
		deposit_info.min_deposit_info.deposit_amount as first_deposit_amount,
		
		deposit_info.max_deposit_info.deposit_time as last_deposit_time,
		deposit_info.max_deposit_info.order_id as last_deposit_order_no,
		deposit_info.max_deposit_info.etc_card as last_deposit_etc_card,
		deposit_info.max_deposit_info.deposit_amount as last_deposit_amount,
		datediff('${datekey}',to_date(deposit_info.max_deposit_info.deposit_time)) as last_deposit_from_days,
		
		deposit.total_deposit_amount,
		deposit.total_deposit_times,
		deposit.total_deposit_etc_cards,
		deposit.last_7d_deposit_amount,
		deposit.last_7d_deposit_times,
		deposit.last_7d_deposit_etc_cards,
		deposit.last_14d_deposit_amount,
		deposit.last_14d_deposit_times,
		deposit.last_14d_deposit_etc_cards,
		deposit.last_30d_deposit_amount,
		deposit.last_30d_deposit_times,
		deposit.last_30d_deposit_etc_cards,
		deposit.last_60d_deposit_amount,
		deposit.last_60d_deposit_times,
		deposit.last_60d_deposit_etc_cards,
		
		case when datediff('${datekey}',to_date(deposit_info.max_deposit_info.deposit_time))>30 
			and datediff('${datekey}',to_date(deposit_info.max_deposit_info.deposit_time))<60 then '沉默用户'
			when datediff('${datekey}',to_date(deposit_info.max_deposit_info.deposit_time))>=60 then '流失用户' 
			when datediff('${datekey}',to_date(deposit_info.max_deposit_info.deposit_time))<=30 then '正常用户'
			else '其他' 
			end as user_type,
		case when deposit.last_30d_deposit_amount>15000 then '头部用户'
			when deposit.last_30d_deposit_amount>6000 and deposit.last_30d_deposit_amount<=15000 then '正常用户'
			when deposit.last_30d_deposit_amount>3000 and deposit.last_30d_deposit_amount<=6000 then '摇摆用户'
			when deposit.last_30d_deposit_amount>=100 and deposit.last_30d_deposit_amount<=3000 then '尝鲜用户'
			else '其他'
			end as deposit_user_type,
		
		login.app_login_7d,
		login.app_login_30d,
		login.app_login_60d,
		pgn.etc_visit_7d,
		pgn.etc_visit_30d,
		pgn.etc_visit_60d,
		case when combo.buyer_id is not null then '是' else '否' end as is_purchase_combo
from(
	select * 
	from dwb.umd_user_base_info_s_d 
	where dt='${datekey}'
	) a
---平台相关卡（开卡和充值）
join(
	select * 
	from dim.etc_user_dim_by_user_s_d 
	where dt='${datekey}'
	) b on a.user_id = b.user_id
---平台开卡信息
left join(
	select p1.user_id,
			max(named_struct('open_time',p2.open_time,'etc_card_no',p2.etc_card_no,'card_name',
				p2.card_name,'van_number',p2.van_number,'van_plate_color',p2.van_plate_color)) as last_open_info,
			min(named_struct('open_time',p2.open_time,'etc_card_no',p2.etc_card_no,'card_name',
				p2.card_name,'van_number',p2.van_number,'van_plate_color',p2.van_plate_color)) as first_open_info
	from(
		select * 
		from dim.etc_user_dim_base_s_d 
		where dt='${datekey}'
		) p1
	join(
		select *
		from dwd.etc_open_order_fact_s_d 
		where dt='${datekey}' 
			and open_platform in ('HCB','YMM')
		) p2 on p1.etc_card_no=p2.etc_card_no
	group by p1.user_id
	) c on a.user_id = c.user_id
----常跑省份、打折省份、推荐卡
left join(
		---推荐省份
	select x.user_id
			  ,concat_ws('|',collect_set(recom_card_name)) recom_card_name
	from(
		   select user_id
				  ,province_name
				  ,recom_card_name
				  ,count(distinct dt) report_days
				  ,row_number() OVER (PARTITION BY user_id ORDER BY count(distinct dt) DESC) AS rowwid
		   	from dw_etc.dwd_etc_user_loc_in
		    where dt between date_sub('${datekey}',59) and '${datekey}'
		   	  and is_recom = 1
		    group by user_id
		   		    ,province_name
		   		    ,recom_card_name
		)x
    where x.rowwid = 1 
    group by x.user_id
	)loc_recom on a.user_id = loc_recom.user_id
left join(
		---近60天常跑省份
	select user_id
		  ,concat_ws('|',collect_set(case when x.rowwid = 1 then province_name end)) first_province_60
		  ,concat_ws('|',collect_set(case when x.rowwid <= 3 then concat('省份:',province_name,',天数:',report_days) end)) top3_loc_info
		  ,round(count(case when is_discount = 1 then 1 end)/count(1),4) contact_ratio
	from (
			select user_id
				,province_name
				,recom_card_name
				,is_discount
				,count(distinct dt) report_days
				,row_number() OVER (PARTITION BY user_id ORDER BY count(distinct dt) DESC) AS rowwid
			from dw_etc.dwd_etc_user_loc_in
			where dt between date_sub('${datekey}',59) and '${datekey}'
			group by user_id
					,province_name
					,recom_card_name
					,is_discount
		)x
	group by user_id 
	)loc60 on a.user_id = loc60.user_id
left join(
		---近90天常跑省份
	select user_id
		  ,concat_ws('|',collect_set(case when x.rowwid = 1 then province_name end)) first_province_90
		  ,concat_ws('|',collect_set(case when x.rowwid <= 3 then concat('省份:',province_name,',天数:',report_days) end)) top3_loc_info
		  ,round(count(case when is_discount = 1 then 1 end)/count(1),4) contact_ratio
	from (
	    select user_id
			  ,province_name
			  ,recom_card_name
			  ,is_discount
			  ,count(distinct dt) report_days
			  ,row_number() OVER (PARTITION BY user_id ORDER BY count(distinct dt) DESC) AS rowwid
	   	from dw_etc.dwd_etc_user_loc_in
	    where dt between date_sub('${datekey}',89) and '${datekey}'
	    group by user_id
	   		    ,province_name
	   		    ,recom_card_name
	   		    ,is_discount
		)x
	group by user_id 
	)loc90 on a.user_id = loc90.user_id
left join(
		---近60天打折省份
	select x.user_id
		  ,concat_ws('|',collect_set(case when x.rowwid = 1 then province_name end)) first_dis_province_60
		  ,concat_ws('|',collect_set(case when x.rowwid = 2 then province_name end)) second_dis_province
		  ,concat_ws('|',collect_set(case when x.rowwid = 3 then province_name end)) third_dis_province
		  ,sum(case when x.rowwid = 1 then report_days end)/sum(case when x.rowwid = 1 then all_report_day end) card_mach_ratio
	from (
	    select user_id
			  ,province_name
			  ,recom_card_name
			  ,count(distinct dt) report_days
			  ,row_number() OVER (PARTITION BY user_id ORDER BY count(distinct dt) DESC) AS rowwid
	   	from dw_etc.dwd_etc_user_loc_in
	    where dt between date_sub('${datekey}',59) and '${datekey}'
	   	  and is_discount = 1
	    group by user_id
	   		    ,province_name
	   		    ,recom_card_name
		)x
	left join (
		select user_id
			  ,count(distinct dt) all_report_day
		from dw_etc.dwd_etc_user_loc_in
	    where dt between date_sub('${datekey}',59) and '${datekey}'
	    group by user_id
		)y on x.user_id = y.user_id
	group by x.user_id 
	)loc_dis60 on a.user_id = loc_dis60.user_id
left join(
		---近90天打折省份
	select x.user_id
		  ,concat_ws('|',collect_set(case when x.rowwid = 1 then province_name end)) first_dis_province_90
		  ,concat_ws('|',collect_set(case when x.rowwid = 2 then province_name end)) second_dis_province
		  ,concat_ws('|',collect_set(case when x.rowwid = 3 then province_name end)) third_dis_province
		  ,sum(case when x.rowwid = 1 then report_days end)/sum(case when x.rowwid = 1 then all_report_day end) card_mach_ratio
	from (
	    select user_id
			  ,province_name
			  ,recom_card_name
			  ,count(distinct dt) report_days
			  ,row_number() OVER (PARTITION BY user_id ORDER BY count(distinct dt) DESC) AS rowwid
	   	 from dw_etc.dwd_etc_user_loc_in
	    where dt between date_sub('${datekey}',89) and '${datekey}'
	   	  and is_discount = 1
	    group by user_id
	   		    ,province_name
	   		    ,recom_card_name
		)x
	left join (
		select user_id
			  ,count(distinct dt) all_report_day
		from dw_etc.dwd_etc_user_loc_in
	    where dt between date_sub('${datekey}',89) and '${datekey}'
	    group by user_id
		)y on x.user_id = y.user_id
	group by x.user_id 
	)loc_dis90 on a.user_id = loc_dis90.user_id
---定位浙江省信息
left join(
	select uid,
		count(case when dt between date_sub('${datekey}',6)  and '${datekey}' then 1 else null end) as last_7d_zj_loc,
		count(case when dt between date_sub('${datekey}',29) and '${datekey}' then 1 else null end) as last_30d_zj_loc,
		count(case when dt between date_sub('${datekey}',59) and '${datekey}' then 1 else null end) as last_60d_zj_loc,
		count(case when dt between date_sub('${datekey}',89) and '${datekey}' then 1 else null end) as last_90d_zj_loc		
	from temporarydb.temp_xuzy_etc_zj_loc
	group by uid
	)loc_zj on a.user_id=loc_zj.uid
---绑卡信息
left join(		
	select   user_id, 
			count(distinct card_no)  as bind_card_cnts,
			concat_ws('|',collect_set(card_no)) bind_card_no
	from ods.etc_h077_user_bind_card_record
	where dt='${datekey}'
		and status=1
	group by user_id
	) bind on a.user_id = bind.user_id
----充值信息
left join(
	select operator_id,
			max(named_struct('deposit_cnt_30',deposit_cnt_30,'etc_card',etc_card_no,'card_name',card_name)) as deposit_info_30,
			max(named_struct('deposit_cnt_60',deposit_cnt_60,'etc_card',etc_card_no,'card_name',card_name)) as deposit_info_60,
			sum(deposit_amt_total) as total_deposit_amount,
			sum(deposit_cnt_total) as total_deposit_times,
			count(distinct etc_card_no) as total_deposit_etc_cards,
			sum(deposit_amt_7) as last_7d_deposit_amount,
			sum(deposit_cnt_7) as last_7d_deposit_times,
			count(distinct case when deposit_cnt_7>0 then etc_card_no end) as last_7d_deposit_etc_cards,
			sum(deposit_amt_14) as last_14d_deposit_amount,
			sum(deposit_cnt_14) as last_14d_deposit_times,
			count(distinct case when deposit_cnt_14>0 then etc_card_no end) as last_14d_deposit_etc_cards,
			sum(deposit_amt_30) as last_30d_deposit_amount,
			sum(deposit_cnt_30) as last_30d_deposit_times,
			count(distinct case when deposit_cnt_30>0 then etc_card_no end) as last_30d_deposit_etc_cards,
			sum(deposit_amt_60) as last_60d_deposit_amount,
			sum(deposit_cnt_60) as last_60d_deposit_times,
			count(distinct case when deposit_cnt_60>0 then etc_card_no end) as last_60d_deposit_etc_cards
	from temporarydb.temp_xuzy_etc_deposit
	group by operator_id
	) deposit on a.user_id = deposit.operator_id
left join(
	select operator_id,
			min(named_struct('deposit_time',deposit_time,'order_id',order_id,'etc_card',etc_card_no,'deposit_amount',order_amount)) as min_deposit_info,
			max(named_struct('deposit_time',deposit_time,'order_id',order_id,'etc_card',etc_card_no,'deposit_amount',order_amount)) as max_deposit_info
	from dwd.etc_deposit_order_fact_s_d 
	where dt='${datekey}'
		and order_status='交易完成'
	group by operator_id
	) deposit_info on a.user_id = deposit_info.operator_id
----购买充值宝
left join(
	select combo.buyer_id
	from(
		select *
		from ods.etc_h088_combo_order
		where dt = '${datekey}'
		  and to_date(create_time) <= '${datekey}'
		  and status = 1
		)combo 
	join dw_etc.dwd_etc_combo_list list on combo.combo_code = list.combo_code
	join(
	    select *
		from ods.etc_h088_order_detail
		where dt = '${datekey}'
		  and product_id in (2045,2039,2018,2013,2001,1008,439,440,441,442,443,457,458,459,460,461,468,474,478,481)
    	)order_detail on combo.order_no = order_detail.order_no
	group by combo.buyer_id
	) combo on a.user_id=combo.buyer_id
left join(
	select user_id,
			count(distinct case when dt between date_sub('${datekey}',6) and '${datekey}' then dt end) as app_login_7d,
			count(distinct case when dt between date_sub('${datekey}',29) and '${datekey}' then dt end) as app_login_30d,
			count(distinct case when dt between date_sub('${datekey}',59) and '${datekey}' then dt end) as app_login_60d
	from dwb.uce_user_session_i_d 
	where dt between date_sub('${datekey}',59) and '${datekey}'
	group by user_id
	) login on a.user_id = login.user_id
left join(
	select uid_,
			count(distinct case when dt between date_sub('${datekey}',6) and '${datekey}' then dt end) as etc_visit_7d,
			count(distinct case when dt between date_sub('${datekey}',29) and '${datekey}' then dt end) as etc_visit_30d,
			count(distinct case when dt between date_sub('${datekey}',59) and '${datekey}' then dt end) as etc_visit_60d
	from ups_prod.dwd_middle_visit_pages 
	where dt between date_sub('${datekey}',59) and '${datekey}'
		and pgn='etc_home_newpage'
	group by uid_
	) pgn on a.user_id = pgn.uid_;

 