CREATE TABLE if not exists `dmt.etc_income_i_d`( 
czb_sale_income     double comment '充值宝销售收入',
xkxh_service_income double comment '销卡销户服务收入',
sctg_fee_coup       double comment '市场推广费（优惠券）',
hk_cost_agent       double comment '获客成本-代理',
hk_cost_dx          double comment '获客成本-电销',
jzk_comsum_amount   double comment '记账卡消费金额',
jzk_consum_cnts     bigint comment '记账卡消费张数',
post_amount         bigint comment '发票邮寄费',
deposit_income     bigint comment '充值收入'

)comment 'ETC日收入表'
partitioned by (dt	string comment 'ETC日期')
STORED AS ORC;


drop table if exists temporarydb.tdl_tmp_combo_sale_income;
create table if not exists temporarydb.tdl_tmp_combo_sale_income as 
select  combo.date_id,
		round(C3ST_sale_number*100+C3HST_sale_number*100+C3SST_sale_number*100+
		C1HDX_sale_number*45+C1DX_sale_number*45+C1S_sale_number*45+
		C11_sale_number*88+C11H_sale_number*88+C11S_sale_number*88+
		ETCJH_sale_number*0+COC3_sale_number*45+COC1_sale_number*100+YTK_sale_number*88+C3_sale_number*100
		+ETCBT_sale_number*45+ETCYFF_sale_number*45+CZBSM_sale_number*100		
		+nvl(647_order_cnt,0)*88+case when combo.date_id<='2019-06-16' and combo.date_id>='2019-06-20' then nvl(661_order_cnt,0)*45 else nvl(661_order_cnt,0)*19.9 end +nvl(716_order_cnt,0)*60,2) as combo_income
from (
	select to_date(create_time) as date_id,
			count(distinct case when combo_code='SJYW2019040101' then order_no end) as C3ST_sale_number,
			count(distinct case when combo_code='SJYW2019041101' then order_no end) as C3HST_sale_number,
			count(distinct case when combo_code='SJYW2019041102' then order_no end) as C3SST_sale_number,
			count(distinct case when combo_code='SJYW2019040201' then order_no end) as C1HDX_sale_number,
			count(distinct case when combo_code='SJYW2019040202' then order_no end) as C1DX_sale_number,
			count(distinct case when combo_code='SJYW2018042801' then order_no end) as C1S_sale_number,
			count(distinct case when combo_code='SJYW2018010401' then order_no end) as C11_sale_number,
			count(distinct case when combo_code='SJYW2018010402' then order_no end) as C11H_sale_number,
			
			count(distinct case when combo_code='SJYW2018042806' then order_no end) as C11S_sale_number,
			count(distinct case when combo_code='SJYW2019041103' then order_no end) as ETCJH_sale_number,
			count(distinct case when combo_code='ETCY032102' 	 then order_no end) as COC3_sale_number,
			count(distinct case when combo_code='ETCY032101' 	 then order_no end) as COC1_sale_number,
			count(distinct case when combo_code='QYET2018012502' then order_no end) as YTK_sale_number,
			count(distinct case when combo_code='BTYW0214C3' 	 then order_no end) as C3_sale_number,
			
			count(distinct case when combo_code='SJYW2019050601' then order_no end) as ETCBT_sale_number,
			count(distinct case when combo_code='SJYW2019042901' then order_no end) as ETCYFF_sale_number,
			count(distinct case when combo_code='QYET2019050601' then order_no end) as CZBSM_sale_number
			
	from ods.etc_h088_combo_order combo
	where combo.dt='${datekey}'
	and to_date(create_time)='${datekey}'
	and combo_code in (
	'SJYW2019040101'
	,'SJYW2019041101'
	,'SJYW2019041102'
	,'SJYW2019040202'
	,'SJYW2019040201'
	,'SJYW2018042801'
	,'SJYW2018010401'
	,'SJYW2018010402'
	,'SJYW2018042806'
	,'SJYW2019041103'
	,'ETCY032102'
	,'ETCY032101'
	,'QYET2018012502'
	,'BTYW0214C3'
	,'SJYW2019050601'
	,'SJYW2019042901'
	,'QYET2019050601'
)
	and status=1
	group by to_date(create_time)
)combo
---point
 left join(
			select to_date(oi.create_time) as date_id,
						count(distinct case when product_id=647 then oi.order_no else null end) as 647_order_cnt,
						count(distinct case when product_id=661 then oi.order_no else null end) as 661_order_cnt,
						count(distinct case when product_id=661 then oi.order_no else null end) as 716_order_cnt
				from ods.app_h346_order_item oi
				LEFT JOIN ods.app_h346_orders o ON oi.order_no = o.order_no and o.dt='${datekey}'
				where oi.dt='${datekey}'
				and oi.product_id in (647,661,716)
				and to_date(oi.create_time)='${datekey}'
				group by to_date(oi.create_time)
	)point on combo.date_id=point.date_id;
	

	

ALTER TABLE dmt.etc_income_i_d  DROP IF EXISTS PARTITION(dt='${datekey}');
insert overwrite table dmt.etc_income_i_d  partition (dt='${datekey}')
select 	sum(czb_sale_income) 		as czb_sale_income,
		sum(xkxh_service_income)	as xkxh_service_income,
		sum(sctg_fee_coup) 			as sctg_fee_coup,
		sum(hk_cost_agent) 			as hk_cost_agent,
		sum(hk_cost_dx) 			as hk_cost_dx,
		sum(jzk_comsum_amount) 		as jzk_comsum_amount,
		sum(jzk_consum_cnts) 		as jzk_consum_cnts,
		sum(post_amount)            as post_amount,
		sum(deposit_income)         as deposit_income
from(
	select date_id,
	  		sum(combo.combo_income) as czb_sale_income,
	  		0 as xkxh_service_income,
	  		0 as sctg_fee_coup,
	  		0 as hk_cost_agent,
	  		0 as hk_cost_dx,
	  		0 as jzk_comsum_amount,
	  		0 as jzk_consum_cnts,
			0 as post_amount,
			0 as deposit_income
	  from  temporarydb.tdl_tmp_20190326_combo_sale_income combo
	  group by date_id
	  
	  
	union all
	  
	select to_date(cancel_time) as date_id,
	  		0 as czb_sale_income,
	  		round(sum(rate),2) as xkxh_service_income,
	  		0 as sctg_fee_coup,
	  		0 as hk_cost_agent,
	  		0 as hk_cost_dx,
	  		0 as jzk_comsum_amount,
	  		0 as jzk_consum_cnts,
			0 as post_amount,
			0 as deposit_income
	  		
	  from ods.etc_h077_cancellation_card
	  where 	dt='${datekey}'
	  and type=1 
	  and to_date(cancel_time)='${datekey}'
	  group by to_date(cancel_time)
	  
	union all
	  
	select b.dt as date_id,
	  		0 as czb_sale_income,
	  		0 as xkxh_service_income,
	  		sum(price) as sctg_fee_coup,
	  		0 as hk_cost_agent,
	  		0 as hk_cost_dx,
	  		0 as jzk_comsum_amount,
	  		0 as jzk_consum_cnts,
			0 as post_amount,
			0 as deposit_income
	  from (
	  	select deposit_order_id
	  	from dw_etc.dw_etc_core_deposit_info 
	  	where dt='${datekey}'
	  	) a
	  join(
	  	select out_order_no,
				dt,
				price
	  	from ods.loan_h153_coupon_consume_detail 
	  	where dt='${datekey}'
	  	and consume_status=0
	  	) b on a.deposit_order_id=b.out_order_no
	  group by b.dt
	  
	union all
	  
	select a.date_id,
	  			0 as czb_sale_income,
	  			0 as xkxh_service_income,
	  			0 as sctg_fee_coup,
	  			case when b.user_type=2 then a.agent_combo_cnt*b.total_cost else 0 end as hk_cost_agent,
	  			case when b.user_type=1 then a.combo_cnt*b.total_cost else 0 end as hk_cost_dx,
	  			0 as jzk_comsum_amount,
	  			0 as jzk_consum_cnts,
				0 as post_amount,
				0 as deposit_income
	  	from dw_etc.combo_cost_daily_i_d  a    ---一个dt下包含了近60天的数据,date_id
	  	join dw_etc.dim_etc_combo_cost b	on a.combo_code=b.combo_code
	  	where a.dt='${datekey}'
		and a.date_id='${datekey}'
	  	
	union all
	  
	  
	select to_date(exit_time) as date_id,
	  		0 as czb_sale_income,
	  		0 as xkxh_service_income,
	  		0 as sctg_fee_coup,
	  		0 as hk_cost_agent,
	  		0 as hk_cost_dx,
	  		sum(document_amount) as jzk_comsum_amount,
	  		count(distinct case when document_amount>0 then card_no else null end) as jzk_consum_cnts,
			0 as post_amount,
			0 as deposit_income
	  
	from ods.etc_h080_tb_qf_bill_detail
	where  dt='${datekey}'
		 and bill_status=0
		 and to_date(exit_time)='${datekey}'
	group by to_date(exit_time)
	
	union all
	
	
	select date_id,
			0 as czb_sale_income,
	  		0 as xkxh_service_income,
	  		0 as sctg_fee_coup,
	  		0 as hk_cost_agent,
	  		0 as hk_cost_dx,
			0 as jzk_comsum_amount,
	  		0 as jzk_consum_cnts,
			post_amount,
			0 as deposit_income
	from dw_etc.dm_etc_invoice_driver_info a
	where dt='${datekey}'
	
	union all
	
	
	select to_date(deposit_time) as date_id,
			0 as czb_sale_income,
	  		0 as xkxh_service_income,
	  		0 as sctg_fee_coup,
	  		0 as hk_cost_agent,
	  		0 as hk_cost_dx,
			0 as jzk_comsum_amount,
	  		0 as jzk_consum_cnts,
			0 as post_amount,
			sum(income) as deposit_income
	from(
		select deposit_time
				,case when product_name = '黔通卡' then round(order_amount*0.001,6)
					when product_name = '三秦通' then round(order_amount*0.0024,6)
					when product_name = '鲁通卡' then round(order_amount*0.0018,6)
					when product_name = '粤通卡' then round(order_amount*0.0013,6)
					when product_name = '八桂行' then round(order_amount*0.003,6)
					---由于苏通卡的费率是阶段性的所以看时间范围算的累计金额后调整的
					---'2017-11-26 11:04:30'  5个亿   2018-04-01 23:18:43 15亿
					when product_name like '苏通%' and deposit_time <= '2017-11-26 11:04:30' then round(order_amount*0.0025,6)
					when product_name like '苏通%' and deposit_time <= '2018-04-01 23:18:43' then round(order_amount*0.002,6)
					when product_name like '苏通%' then round(order_amount*0.0016,6)
					when product_name = '蜀通卡' then 1
			end income		
		from  dwd.etc_deposit_order_fact_s_d a 
		left join dim.etc_card_dim_s_d b on a.etc_card_no=b.etc_card_no and b.dt='${datekey}'
		where a.dt='${datekey}'
		and to_date(a.deposit_time)='${datekey}'
	)deposit
	group by to_date(deposit_time)	
)income;