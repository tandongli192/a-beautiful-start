CREATE TABLE IF NOT EXISTS `dws.dwb_etc_cardtype_daily_i_d`( 
date_id                 string comment '日期',
card_type               string comment 'ETC卡种(储值卡/记账卡)',
open_cards 				bigint comment '开卡数',
ac_open_cards           bigint comment '月累计开卡数',
active_cards 			bigint comment '激活卡数(首充)',
active_persons 			bigint comment '激活人数(首充)',
active_deposit_amt 		bigint comment '激活充值金额(首充)',
active_cards_ach 		bigint comment '激活卡数(业绩)',
active_persons_ach 		bigint comment '激活人数(业绩)',
active_deposit_amt_ach 	double comment '激活充值金额(业绩)',
cancel_cards 			bigint comment '销卡数',
replace_cards 			bigint comment '补卡数',
change_cards 			bigint comment '换卡数',
deposit_cards 			bigint comment '充值卡数',
deposit_times 			bigint comment '充值次数',
deposit_persons 		bigint comment '充值人数',
deposit_amount 			double comment '充值金额'

)comment'按日汇总统计表'
partitioned by (dt string) 
stored as orc;



ALTER TABLE dws.dwb_etc_cardtype_daily_i_d DROP IF EXISTS PARTITION(dt='${datekey}'); 
insert overwrite table dws.dwb_etc_cardtype_daily_i_d partition(dt= '${datekey}')
select date_id,
		card_type,
		sum(open_cards) 	   as open_cards,
		sum(ac_open_cards)	   as ac_open_cards,
		sum(active_cards)	   as active_cards,
		sum(active_persons)	   as active_persons,
		sum(active_deposit_amt)as active_deposit_amt,
		sum(active_cards_ach)  as 	active_cards_ach,
		sum(active_persons_ach)	as	active_persons_ach,
		sum(active_deposit_amt_ach) as active_deposit_amt_ach,
		sum(cancel_cards)	   as cancel_cards,
		sum(replace_cards)	   as replace_cards,
		sum(change_cards)	   as change_cards,
		sum(deposit_cards)	   as deposit_cards,
		sum(deposit_times)	   as deposit_times,
		sum(deposit_persons)   as deposit_persons,
		sum(deposit_amount)	   as deposit_amount
from (
	select to_date(open.open_time) as date_id,
			card.card_type,
			count(open.etc_card_no) as open_cards,
			0 as ac_open_cards,
			0 as active_cards,
			0 as active_persons,
			0 as active_deposit_amt,
			0 as active_cards_ach,
			0 as active_persons_ach,
			0 as active_deposit_amt_ach,
			0 as cancel_cards,
			0 as replace_cards,
			0 as change_cards,
			0 as deposit_cards,
			0 as deposit_times,
			0 as deposit_persons,
			0 as deposit_amount
			
	from  dwd.etc_open_order_fact_s_d open 
	join dim.etc_card_dim_s_d card on open.etc_card_no=card.etc_card_no and card.dt='${datekey}'
	where open.dt='${datekey}'
	and to_date(open.open_time)='${datekey}'
	group by to_date(open.open_time),
			card.card_type
			
	
	union all
	
	
	select '${datekey}' as date_id,
			card.card_type,
			0 as open_cards,
			count(open.etc_card_no) as ac_open_cards,
			0 as active_cards,
			0 as active_persons,
			0 as active_deposit_amt,
			0 as active_cards_ach,
			0 as active_persons_ach,
			0 as active_deposit_amt_ach,
			0 as cancel_cards,
			0 as replace_cards,
			0 as change_cards,
			0 as deposit_cards,
			0 as deposit_times,
			0 as deposit_persons,
			0 as deposit_amount
	from  dwd.etc_open_order_fact_s_d open 
	join dim.etc_card_dim_s_d card on open.etc_card_no=card.etc_card_no and card.dt='${datekey}'
	where open.dt='${datekey}'
	and date_format(open.open_time,'yyyy-MM')=date_format('${datekey}','yyyy-MM')
	group by card.card_type
	
	union all
	
	select to_date(x.active_info.active_time) as date_id,
			card.card_type,
			0 as open_cards,
			0 as ac_open_cards,
			count(distinct x.etc_card_no)   as active_cards,
			count(distinct x.active_info.deposit_user_id) as active_persons,
			sum(x.active_info.deposit_amount) 			as active_deposit_amt,
			0 as active_cards_ach,
			0 as active_persons_ach,
			0 as active_deposit_amt_ach,
			0 as cancel_cards,
			0 as replace_cards,
			0 as change_cards,
			0 as deposit_cards,
			0 as deposit_times,
			0 as deposit_persons,
			0 as deposit_amount
	from(
		select etc_card_no,
				min(named_struct('active_time',deposit_time,'deposit_amount',order_amount,'deposit_user_id',operator_id)) as active_info		
		from dwd.etc_deposit_order_fact_s_d
		where dt='${datekey}'
		group by etc_card_no
	)x
	left join dim.etc_card_dim_s_d card on x.etc_card_no=card.etc_card_no and card.dt='${datekey}'
	where to_date(x.active_info.active_time)='${datekey}'
	group by to_date(x.active_info.active_time),
			card.card_type
			
	union all
	
	
	select 	to_date(active.perf_active_time) as date_id,
			card.card_type,
			0 as open_cards,
			0 as ac_open_cards,
			0 as active_cards,
			0 as active_persons,
			0 as active_deposit_amt,
			count(distinct active.etc_card) 				as active_cards_ach,
			count(distinct active.perf_active_emp_user_id) as active_persons_ach,
			sum(active.active_amount) 						        as active_deposit_amt_ach,
			0 as cancel_cards,
			0 as replace_cards,
			0 as change_cards,
			0 as deposit_cards,
			0 as deposit_times,
			0 as deposit_persons,
			0 as deposit_amount	
	from dws.etc_core_performance_acitve_s_d active 
	left join dim.etc_card_dim_s_d card on active.etc_card=card.etc_card_no and card.dt='${datekey}'
	where active.dt='${datekey}'
	and to_date(active.perf_active_time)='${datekey}'
	group by to_date(active.perf_active_time),
			card.card_type
	
	
	union all
	
		
	select to_date(cancel.cancel_time) as date_id,
			card.card_type,
			0 as open_cards,
			0 as ac_open_cards,
			0 as active_cards,
			0 as active_persons,
			0 as active_deposit_amt,
			0 as active_cards_ach,
			0 as active_persons_ach,
			0 as active_deposit_amt_ach,
			count(distinct case when cancel.type=1 then cancel.etc_card_no else null end) as cancel_cards,
			count(distinct case when cancel.type=0 then cancel.etc_card_no else null end) as replace_cards,
			count(distinct case when cancel.type=2 then cancel.etc_card_no else null end) as change_cards,
			0 as deposit_cards,
			0 as deposit_times,
			0 as deposit_persons,
			0 as deposit_amount
	from dwd.etc_cancel_order_fact_s_d cancel
	join dim.etc_card_dim_s_d card on cancel.etc_card_no=card.etc_card_no and card.dt='${datekey}'
	where cancel.dt='${datekey}'
	and to_date(cancel.cancel_time)='${datekey}'
	group by to_date(cancel.cancel_time) ,
			card.card_type
			
			
	union all
	
		
	select to_date(deposit.deposit_time) as date_id,
			card.card_type,
			0 as open_cards,
			0 as ac_open_cards,
			0 as active_cards,
			0 as active_persons,
			0 as active_deposit_amt,
			0 as active_cards_ach,
			0 as active_persons_ach,
			0 as active_deposit_amt_ach,
			0 as cancel_cards,
			0 as replace_cards,
			0 as change_cards,
			count(distinct deposit.etc_card_no) as deposit_cards,
			count(1) 					as deposit_times,
			count(distinct deposit.operator_id) as deposit_persons,
			sum(order_amount) 			as deposit_amount
	from dwd.etc_deposit_order_fact_s_d deposit 
	join dim.etc_card_dim_s_d card on deposit.etc_card_no=card.etc_card_no and card.dt='${datekey}'
	where deposit.dt='${datekey}'
	and to_date(deposit.deposit_time)='${datekey}'
	group by to_date(deposit.deposit_time),
			card.card_type
)x
group by date_id,
		card_type;