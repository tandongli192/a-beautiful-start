
----销卡事实表
create table if not exists dwd.etc_cancel_order_fact_s_d
(
trade_id       			string	comment '退款交易唯一ID'
,cancel_order_id		string	comment	'销卡订单号'
,etc_card_no			string  comment	'ETC卡号'
,status					int 	comment	'销卡订单状态(0待审核/1已支付/2已销卡/3已退款/4取消申请/5取消成功/6销卡过来的取消申请/7销卡过来的取消申请成功/8审核不通过/9已转卡(针对补换卡)/10审核通过/11提交中)'
,type					int		comment	'销卡类型(0-补卡/1-销卡/2-换卡)'
,have_overage			int		comment	'是否有余额(0-没有/1-有)'
,reason_id				int		comment	'补换卡原因(0未知原因/1卡片丢失/2卡片无法读取/3更换至其他省市卡片/4不再使用ETC卡/5其他原因/6车子不跑需要销户/7区域没有折扣,办理其他ETC卡)'
,reason_name			string	comment	'补换卡原因'
,rate  					double	comment	'销卡手续费金额'
,pay_status				int 	comment '销卡支付状态(1未付款/2已付款)' 
,pay_time				string	comment	'手续费支付时间'
,us_apply_id			bigint	comment	'申请销卡员工ID'
,us_cancel_id			bigint	comment '销卡员工ID'
,cancel_create_time		string	comment	'销卡订单创建时间'
,cancel_update_time		string	comment	'销卡最后一次修改时间'
,id_photo_front			string	comment	'身份证正面'
,id_photo_back			string	comment	'身份证反面'
,driving_license_photo	string	comment	'行驶证'
,cancel_time			string	comment	'销卡时间'
,refuse_reason			string	comment	'拒绝原因'
,cancel_progress_status	int		comment	'销卡进度状态(0-默认值/1-进行中/2-完结)'
,has_card				int		comment	'针对是否有卡收费(0-默认,有卡无卡都收该项费用/1-有卡/2-无卡)'
,customer_type			int		comment	'用户类型(1-个人/2-企业)'
,is_delete				int		comment	'销卡删除标识(0-未删除/1-已删除)'
,sele_process			int		comment	'销卡退费流程选择(0-老流程/1-电子协议流程)'
,app_channel			int		comment	'充值App区分(0管理版/1司机版/2企业版)'
,pass_time				string	comment	'销卡审核通过时间(默认是:1900-01-01 00:00:00.0)'
,product_id             string  comment '商品唯一ID'
,order_id   			string	comment	'退款订单号'
,refund_time			string	comment	'退费时间(货车帮财务打款给用户的时间)'
,refund_amount			double	comment	'实退金额(货车帮财务打款给用户的金额)'
,order_status			string 	comment	'货车帮退款状态'
,refund_comment         string  comment '退款备注'
,user_name				string	comment	'持卡人姓名'
,ic_no					string	comment	'身份证号'
,bank_province			string	comment	'收款人银行卡开户省份'
,bank_city				string	comment	'收款人银行卡开户市'
,bank_area				string	comment	'收款人区域'
,bank_name				string	comment	'收款人银行名称'
,bank_branch_name		string	comment	'收款人支行名称'
,bank_user_name			string	comment	'收款人开户名'
,bank_card_no			string	comment	'收款人银行卡号'
,comment				string	comment	'销卡备注'
,operator_id			string	comment	'退款员工ID'
,service_type  			int 	comment '退款服务类型(0-普通退款/1-极速退款/默认值0,其他值参见服务类型表)'
,available_amount		double	comment	'etc卡金额'
,fee					double	comment	'业务收费'
,create_time			string	comment	'退款订单创建时间'
,update_time			string	comment	'退款订单最后一次修改时间'
,refund_status			int		comment	'同步高速状态(0-初始/1-申请成功/2-申请失败/3-审核不通过)'
,refund_is_delete		int		comment	'退款订单删除标识(0-未删除/1-已删除)'
,third_progress			int		comment	'第三方退款进度状态(-1默认值/0-申请/1-确认/2-已退款/3-审核通过/4-审核不通过/5-已转账/6-已退回/7-齐鲁退款申请成功/8-齐鲁退款申请失败/9-齐鲁退款申请接口无回应/10-齐鲁退款确认成功/11-齐鲁退款确认失败/12-齐鲁退款确认无回应)'
,third_right_amount		double	comment	'应退金额(高速返回的应退金额)'
,receipt_no				string	comment	'回单编号(货车帮财务生成的)'
,third_real_amount		double	comment	'回款金额(高速打款给货车帮的金额)'
,third_pay_time			string	comment	'回款时间(高速打款给货车帮的时间)'
,refund_remark			string	comment	'同步高速状态备注'
,speed_progress			int		comment	'高速进度(产品定义的)(默认值0/1未退款/2可退款/3退款失败/4已打款)'
,modify_count			int		comment	'修改次数'
,order_gmv_amount       double  comment '单表订单的GMV金额'
,bussiness_scope		string	comment '交易所属业务线(车油/ETC/保险/套餐)'
,bussiness_topic		string	comment '交易所属子业务'

)comment 'ETC销卡事实表'
partitioned by (dt	string comment '日期')
STORED AS ORC
TBLPROPERTIES("creator"="zhiyuan.xu@56qq.com","safe_level"="C3","importance"="重要");


alter table dwd.etc_cancel_order_fact_s_d drop partition(dt='${datekey}');

insert overwrite table dwd.etc_cancel_order_fact_s_d partition(dt='${datekey}')
select dim.mask(concat(order_id,'ETC业务','ETC销卡'), 'ENCRY') as trade_id      			
	   ,order_id as cancel_order_id		
       ,card_no as etc_card_no			
       ,status					
       ,type 				
       ,have_overage			
       ,reason_id				
       ,reason_name			
       ,rate  					
       ,case when to_date(pay_time)='1900-01-01' then 1 else 2 end pay_status				
       ,pay_time				
       ,split(us_apply_id,'_')[1]  as us_apply_id			
       ,split(us_cancel_id,'_')[1] as us_cancel_id			
       ,add_time as cancel_create_time		
       ,modify_time as cancel_update_time		
       ,id_photo_front			
       ,id_photo_back			
       ,driving_license_photo		
       ,cancel_time			
       ,refuse_reason					
       ,cancel_progress_status	
       ,has_card				
       ,customer_type			
       ,is_delete				
       ,sele_process			
       ,app_channel			
       ,pass_time				
       ,dim.mask(concat(card_no,'ETC业务','ETC'),'ENCRY')  as product_id             
       ,null as order_id   			
       ,null as refund_time			
       ,null as refund_amount			
       ,null as order_status
	   ,null as refund_comment
       ,user_name				
       ,credential_no as ic_no					
       ,bank_province			
       ,bank_city				
       ,bank_area				
       ,bank_name				
       ,bank_branch_name		
       ,bank_user_name			
       ,bank_card_no			
       ,comment				
       ,null as operator_id			
       ,null as service_type  			
       ,etc_card_balance as available_amount		
       ,0.0 as fee					
       ,null as create_time			
       ,null as update_time			
       ,null as refund_status			
       ,null as refund_is_delete		
       ,null as third_progress			
       ,0.0  as third_right_amount		
       ,null as receipt_no				
       ,null as third_real_amount		
       ,null as third_pay_time			
       ,null as refund_remark			
       ,null as speed_progress			
       ,null as modify_count			
       ,0.0  as order_gmv_amount       
       ,'ETC业务' as bussiness_scope
	   ,'ETC销卡订单' as bussiness_topic	
from  ods.etc_h077_cancellation_card cancel 
where dt='${datekey}'
and have_overage=0

union all

select dim.mask(concat(refund.order_no,'ETC业务','ETC余额退款'), 'ENCRY') as trade_id
		,cancel.order_id as cancel_order_id		
		,refund.card_no as etc_card_no			
		,cancel.status					
		,cancel.type				
		,cancel.have_overage			
		,cancel.reason_id				
		,cancel.reason_name			
		,cancel.rate  					
		,case when to_date(cancel.pay_time)='1900-01-01' then 1 else 2 end pay_status
		,cancel.pay_time				
		,split(cancel.us_apply_id,'_')[1] as 	us_apply_id		
		,split(cancel.us_cancel_id,'_')[1] as us_cancel_id			
		,cancel.add_time as cancel_create_time		
		,cancel.modify_time as cancel_update_time		
		,cancel.id_photo_front			
		,cancel.id_photo_back			
		,cancel.driving_license_photo		
		,cancel.cancel_time			
		,cancel.refuse_reason					
		,cancel.cancel_progress_status	
		,cancel.has_card				
		,cancel.customer_type			
		,cancel.is_delete				
		,cancel.sele_process			
		,cancel.app_channel			
		,cancel.pass_time				
		,dim.mask(concat(refund.card_no,'ETC业务','ETC'), 'ENCRY')  as product_id             
		,refund.order_no as order_id   			
		,refund.pay_time as refund_time			
		,refund.real_amount as refund_amount			
		,manual.field_map_value as order_status	
		,refund.comment as refund_comment
		,refund.user_name				
		,refund.id_no as ic_no						
		,refund.bank_province			
		,refund.bank_city				
		,refund.bank_area				
		,refund.bank_name				
		,refund.bank_branch_name		
		,refund.bank_user_name			
		,refund.bank_card_no			
		,cancel.comment				
		,refund.operate_user_id as operator_id			
		,refund.service_type  			
		,refund.etc_card_balance as available_amount	
		,refund.fee					
		,refund.add_time as create_time
		,refund.modify_time as update_time	
		,refund.refund_status			
		,refund.is_delete as refund_is_delete		
		,refund.third_progress			
		,refund.third_right_amount		
		,refund. receipt_no				
		,refund. third_real_amount		
		,refund. third_pay_time			
		,refund. refund_remark			
		,refund. speed_progress			
		,refund. modify_count			
		,0.0 as order_gmv_amount       
		,'ETC' as bussiness_scope
		,'ETC余额退款' as bussiness_topic		

from ods.etc_h077_cancellation_card cancel 
join ods.etc_h074_cancel_refund_order refund on refund.source_order_no=cancel.order_id and  refund.dt='${datekey}'
left join dim.dim_field_enum_des_manual manual on cast(refund.pay_status as string)=manual.field_key  and manual.table_name='etc_h074_cancel_refund_order'and manual.dt='${datekey}'
where cancel.dt='${datekey}'
and cancel.have_overage=1;