set hive.exec.dynamic.partition.mode=nonstrict;	

create table if not exists dwd.etc_deposit_order_fact_s_d(
trade_id			string	comment '交易唯一ID',
order_id		   	string	comment '订单号',
order_amount		double	comment '充值金额',
deposit_time        string  comment '充值时间',
order_status	  	string	comment '订单状态',
origin_status       int     comment '订单原始状态',
create_time			string	comment '订单创建时间',
update_time			string	comment '订单更新时间',
pay_amount_due		double	comment '应付款金额',
pay_amount			double	comment '实际付款金额',
channel			   	bigint	comment '充值渠道:1-管理版 / 2-司机版',
deposit_type      	int     comment '充值类型：0：个人充值；1：企业充值',
type			   	bigint	comment '订单类型:1-正常订单 / 2-三秦通转卡订单',
order_plateform		string	comment  '订单来源平台(HCB,YMM)',
order_channel		string	comment  '订单渠道(HCB管理版、HCB司机版、HCB货主版、HCB企业版、YMM司机版、YMM货主版、YMM企业版、微信小程序、微信)',
pay_channel			string	comment '支付渠道',
van_number		   	string	comment '车牌号',
product_id         string	comment '商品唯一ID',
operator_id		   string	comment '操作人用户ID',
operator_ymm_id    string   comment '操作人ymmID',
apply_time		   string	comment '申请写卡客户端时间',
etc_card_no		   string	comment 'ETC卡号',
card_name           string  comment '卡名称',
available_amount   double	comment '卡片余额',
system_trace	   string	comment 'VFJ系统流水',
tac_nubmer		   string	comment 'tac码',
card_bill		   string	comment '卡交易流水号',
trade_bill		   string	comment '交易流水号',
device_no		   string	comment '终端设备号',
device_bill		   string	comment '设备终端流水',
pay_method		   string	comment '支付方式',
pay_account		   string	comment '支付账号',
pay_time		   string	comment '支付时间',
deposit_location   string	comment '充值经纬度',
deposit_address	   string	comment '充值地址',
deposit_app		   string	comment '充值app名字',
deposit_channel	   string	comment '充值设备通道',
finger_print	   string	comment '设备指纹',
refund_amount	   double	comment '退款金额',
refund_time		   string	comment '退款时间',
refund_desc		   string	comment '退款原因',
pay_identify_code  string	comment '支付凭证号',
sn				   string	comment '圈存设备SN号',
etc_ch_trade_no	   string	comment 'etc渠道中心交易号(目前只用于三秦通)',
appname			   int		comment '充值App区分,0管理版,1司机版,2企业版',
company_id         int   	comment '充值企业id',
order_gmv_amount   double	comment '单表订单的GMV金额',
bussiness_scope	   string	comment '交易所属业务线(车油/ETC/保险/套餐)',
bussiness_topic	   string	comment '交易所属子业务'
)comment 'ETC充值订单表'
partitioned by (dt	string comment 'ETC日期')
STORED AS ORC
TBLPROPERTIES("creator"="zhiyuan.xu@56qq.com","safe_level"="C3","importance"="重要");

alter table dwd.etc_deposit_order_fact_s_d drop partition(dt='${datekey}');
insert into dwd.etc_deposit_order_fact_s_d partition(dt='${datekey}')
select 
		dim.mask(concat(orders.order_id,'ETC业务','ETC充值业务'), 'ENCRY') as trade_id,
		orders.order_id,
		orders.deposit_amount as order_amount,
		orders.deposit_time,
		manual.field_map_value as  order_status,
		manual.field_key as origin_status,
		orders.add_time as create_time,
		orders.modify_time as update_time,
		orders.deposit_amount as pay_amount_due,
		orders.deposit_amount as pay_amount,
		orders.channel,
		case when company.order_no is null then 0 else 1 end as deposit_type,
		orders.type,
		case when orders.channel in (1,2) then 'HCB'
			 when orders.channel = 3 then 'YMM' end as order_plateform,
		concat(case when orders.channel in (1,2) then 'HCB' when orders.channel = 3 then 'YMM' end,
				case when orders.app_channel = 0 then '管理版'  when orders.app_channel = 1 then '司机版' when orders.app_channel = 2 then '企业版' end) as order_channel,
		case when orders.pay_method like '%ABC%' then '中国农业银行'
			 when orders.pay_method like '%ALIPAY%' then '支付宝'
			 when orders.pay_method like '%BAITIAO%' then '白条支付'
			 when orders.pay_method like '%BCCB%' then '北京银行'
			 when orders.pay_method like '%BOC%' then '中国银行'
			 when orders.pay_method like '%CCB%' then '中国建设银行'
			 when orders.pay_method like '%CEB%' then '中国光大银行'
			 when orders.pay_method like '%CIB%' then '兴业银行'
			 when orders.pay_method like '%CITIC%' then '中信银行'
			 when orders.pay_method like '%CMB%' then '招商银行'
			 when orders.pay_method like '%CMBC%' then '中国民生银行'
			 when orders.pay_method like '%COMM%' then '中国交通银行'
			 when orders.pay_method like '%GDB%' then '广东发展银行'
			 when orders.pay_method like '%HXB%' then '华夏银行'
			 when orders.pay_method like '%ICBC%' then '中国工商银行'
			 when orders.pay_method like '%PAYECO%' then '易联支付'
			 when orders.pay_method like '%PSBC%' then '中国邮政储蓄银行'
			 when orders.pay_method like '%SDB%' then '深圳发展银行'
			 when orders.pay_method like '%SPDB%' then '上海浦东发展银行'
			 when orders.pay_method like '%SZPAB%' then '平安银行'
			 when orders.pay_method like '%UROVOPOS%' then '银联POS机'
			 when orders.pay_method like '%WXPAY%' then '微信支付'
			 when orders.pay_method like '%YEEPAY%' then '易宝支付'
			 when orders.pay_method like '%BALANCE%' then '钱包、余额'
			 when orders.pay_method like '%ETC_ACCOUNT%' then 'ETC专项金'
			 when orders.pay_method like '%BANKCARD_QPAY%' then '快捷支付'
			 when orders.pay_method like '%BANK_WEB%' then 'web页面支付'
			 when orders.pay_method like '%BOS%' then '上海银行'
			 when orders.pay_method like '%COUPON%' then '卡券支付'
			 else orders.pay_method end as pay_channel,
		orders.van_number,
		dim.mask(concat(orders.etc_card,'ETC业务','ETC卡'),'ENCRY') as product_id,
		split(orders.operate_id,'_')[1] as operator_id,
		b.ymm_uid as operator_ymm_id,
		if(to_date(orders.apply_time)='1900-01-01',orders.modify_time,orders.apply_time) as apply_time,
		orders.etc_card as etc_card_no,
		null as card_name,        
		orders.blance as available_amount,
		orders.system_trace,
		orders.tac_nubmer,
		orders.card_bill,
		orders.trade_bill,
		orders.device_no,
		orders.device_bill,
		orders.pay_method,
		orders.pay_account,
		pay_time,
		orders.deposit_location,
		orders.deposit_address,
		orders.deposit_app,
		orders.deposit_channel,
		orders.finger_print,
		refund.refund_amount as refund_amount, 	
		refund.apply_time as refund_time, 		
		refund.desc as redund_desc,	
		orders.pay_identify_code,
		orders.sn,
		orders.etc_ch_trade_no,
		orders.app_channel as appname,
		company.company_id, 
		orders.deposit_amount  as order_gmv_amount,
		'ETC业务' as bussiness_scope,
		'ETC充值订单' as bussiness_topic

	from ods.etc_h077_deposit_order  orders
	left join ods.etc_h077_apply_refund  refund on orders.order_id=refund.pre_order_id and refund.dt='${datekey}' and refund.status=2
	left join dw_etc.dw_etc_company_deposit_order_new company on orders.order_id=company.order_no and company.dt='${datekey}'
	left join dim.dim_field_enum_des_manual manual on cast(orders.status as string)=manual.field_key  and manual.table_name='etc_h077_deposit_order'and manual.dt='2019-04-24'
	left join dwb.umd_user_base_info_s_d b on split(orders.operate_id, '_')[1]=b.user_id and b.dt='${datekey}'
	where orders.dt='${datekey}' 
	and orders.app_channel!=0
	and split(orders.operate_id, '_')[0]=1

union all

select 
		dim.mask(concat(orders.order_id,'ETC业务','ETC充值业务'), 'ENCRY') as trade_id,
		orders.order_id,
		orders.deposit_amount as order_amount,
		orders.deposit_time,
		manual.field_map_value as  order_status,
		manual.field_key as origin_status,
		orders.add_time as create_time,
		orders.modify_time as update_time,
		orders.deposit_amount as pay_amount_due,
		orders.deposit_amount as pay_amount,
		orders.channel,
		case when company.order_no is null then 0 else 1 end as deposit_type,
		orders.type,
		case when orders.channel in (1,2) then 'HCB'
			 when orders.channel = 3 then 'YMM' end as order_plateform,
		concat(case when orders.channel in (1,2) then 'HCB' when orders.channel = 3 then 'YMM' end,
				case when orders.app_channel = 0 then '管理版'  when orders.app_channel = 1 then '司机版' when orders.app_channel = 2 then '企业版' end) as order_channel,
		case when orders.pay_method like '%ABC%' then '中国农业银行'
			 when orders.pay_method like '%ALIPAY%' then '支付宝'
			 when orders.pay_method like '%BAITIAO%' then '白条支付'
			 when orders.pay_method like '%BCCB%' then '北京银行'
			 when orders.pay_method like '%BOC%' then '中国银行'
			 when orders.pay_method like '%CCB%' then '中国建设银行'
			 when orders.pay_method like '%CEB%' then '中国光大银行'
			 when orders.pay_method like '%CIB%' then '兴业银行'
			 when orders.pay_method like '%CITIC%' then '中信银行'
			 when orders.pay_method like '%CMB%' then '招商银行'
			 when orders.pay_method like '%CMBC%' then '中国民生银行'
			 when orders.pay_method like '%COMM%' then '中国交通银行'
			 when orders.pay_method like '%GDB%' then '广东发展银行'
			 when orders.pay_method like '%HXB%' then '华夏银行'
			 when orders.pay_method like '%ICBC%' then '中国工商银行'
			 when orders.pay_method like '%PAYECO%' then '易联支付'
			 when orders.pay_method like '%PSBC%' then '中国邮政储蓄银行'
			 when orders.pay_method like '%SDB%' then '深圳发展银行'
			 when orders.pay_method like '%SPDB%' then '上海浦东发展银行'
			 when orders.pay_method like '%SZPAB%' then '平安银行'
			 when orders.pay_method like '%UROVOPOS%' then '银联POS机'
			 when orders.pay_method like '%WXPAY%' then '微信支付'
			 when orders.pay_method like '%YEEPAY%' then '易宝支付'
			 when orders.pay_method like '%BALANCE%' then '钱包、余额'
			 when orders.pay_method like '%ETC_ACCOUNT%' then 'ETC专项金'
			 when orders.pay_method like '%BANKCARD_QPAY%' then '快捷支付'
			 when orders.pay_method like '%BANK_WEB%' then 'web页面支付'
			 when orders.pay_method like '%BOS%' then '上海银行'
			 when orders.pay_method like '%COUPON%' then '卡券支付'
			 else orders.pay_method end as pay_channel,
		orders.van_number,
		dim.mask(concat(orders.etc_card,'ETC业务','ETC卡'),'ENCRY') as product_id,
		b.user_id as operator_id,
		split(orders.operate_id,'_')[1] as operator_ymm_id,
		if(to_date(orders.apply_time)='1900-01-01',orders.modify_time,orders.apply_time) as apply_time,
		orders.etc_card as etc_card_no,
		null as card_name,         
		orders.blance as available_amount,
		orders.system_trace,
		orders.tac_nubmer,
		orders.card_bill,
		orders.trade_bill,
		orders.device_no,
		orders.device_bill,
		orders.pay_method,
		orders.pay_account,
		pay_time,
		orders.deposit_location,
		orders.deposit_address,
		orders.deposit_app,
		orders.deposit_channel,
		orders.finger_print,
		refund.refund_amount as refund_amount, 	
		refund.apply_time as refund_time, 		
		refund.desc as redund_desc,	
		orders.pay_identify_code,
		orders.sn,
		orders.etc_ch_trade_no,
		orders.app_channel as appname,
		company.company_id, 
		orders.deposit_amount  as order_gmv_amount,
		'ETC业务' as bussiness_scope,
		'ETC充值订单' as bussiness_topic

	from ods.etc_h077_deposit_order  orders
	left join ods.etc_h077_apply_refund  refund on orders.order_id=refund.pre_order_id and refund.dt='${datekey}' and refund.status=2
	left join dw_etc.dw_etc_company_deposit_order_new company on orders.order_id=company.order_no and company.dt='${datekey}'
	left join dim.dim_field_enum_des_manual manual on cast(orders.status as string)=manual.field_key  and manual.table_name='etc_h077_deposit_order'and manual.dt='2019-04-24'
	left join dwb.umd_user_base_info_s_d b on split(orders.operate_id, '_')[1]=b.ymm_uid and b.dt='${datekey}'
	where orders.dt='${datekey}' 
	and orders.app_channel!=0
	and split(orders.operate_id, '_')[0]=99
	
	
union all

--再插入管理版数据
select 
		dim.mask(concat(orders.order_id,'ETC业务','ETC充值业务'), 'ENCRY') as trade_id,
		orders.order_id,
		orders.deposit_amount as order_amount,
		orders.deposit_time,
		manual.field_map_value as  order_status,
		manual.field_key as origin_status,
		orders.add_time as create_time,
		orders.modify_time as update_time,
		orders.deposit_amount as pay_amount_due,
		orders.deposit_amount as pay_amount,
		orders.channel,
		case when company.order_no is null then 0 else 1 end as deposit_type,
		orders.type,
		case when orders.channel in (1,2) then 'HCB'
			 when orders.channel = 3 then 'YMM' end as order_plateform,
		concat(case when orders.channel in (1,2) then 'HCB' when orders.channel = 3 then 'YMM' end,
				case when orders.app_channel = 0 then '管理版'  when orders.app_channel = 1 then '司机版' when orders.app_channel = 2 then '企业版' end) as order_channel,
		case when orders.pay_method like '%ABC%' then '中国农业银行'
			 when orders.pay_method like '%ALIPAY%' then '支付宝'
			 when orders.pay_method like '%BAITIAO%' then '白条支付'
			 when orders.pay_method like '%BCCB%' then '北京银行'
			 when orders.pay_method like '%BOC%' then '中国银行'
			 when orders.pay_method like '%CCB%' then '中国建设银行'
			 when orders.pay_method like '%CEB%' then '中国光大银行'
			 when orders.pay_method like '%CIB%' then '兴业银行'
			 when orders.pay_method like '%CITIC%' then '中信银行'
			 when orders.pay_method like '%CMB%' then '招商银行'
			 when orders.pay_method like '%CMBC%' then '中国民生银行'
			 when orders.pay_method like '%COMM%' then '中国交通银行'
			 when orders.pay_method like '%GDB%' then '广东发展银行'
			 when orders.pay_method like '%HXB%' then '华夏银行'
			 when orders.pay_method like '%ICBC%' then '中国工商银行'
			 when orders.pay_method like '%PAYECO%' then '易联支付'
			 when orders.pay_method like '%PSBC%' then '中国邮政储蓄银行'
			 when orders.pay_method like '%SDB%' then '深圳发展银行'
			 when orders.pay_method like '%SPDB%' then '上海浦东发展银行'
			 when orders.pay_method like '%SZPAB%' then '平安银行'
			 when orders.pay_method like '%UROVOPOS%' then '银联POS机'
			 when orders.pay_method like '%WXPAY%' then '微信支付'
			 when orders.pay_method like '%YEEPAY%' then '易宝支付'
			 when orders.pay_method like '%BALANCE%' then '钱包、余额'
			 when orders.pay_method like '%ETC_ACCOUNT%' then 'ETC专项金'
			 when orders.pay_method like '%BANKCARD_QPAY%' then '快捷支付'
			 when orders.pay_method like '%BANK_WEB%' then 'web页面支付'
			 when orders.pay_method like '%BOS%' then '上海银行'
			 when orders.pay_method like '%COUPON%' then '卡券支付'
			 else orders.pay_method end as pay_channel,
		orders.van_number,
		dim.mask(concat(orders.etc_card,'ETC业务','ETC卡'),'ENCRY') as product_id,
		split(orders.operate_id,'_')[1] as operator_id,
		user_info.ymm_uid as operator_ymm_id,
		
		if(to_date(orders.apply_time)='1900-01-01',orders.modify_time,orders.apply_time) as apply_time,
		orders.etc_card as etc_card_no,
		null  as card_name,
		orders.blance as available_amount,
		orders.system_trace,
		orders.tac_nubmer,
		orders.card_bill,
		orders.trade_bill,
		orders.device_no,
		orders.device_bill,
		orders.pay_method,
		orders.pay_account,
		pay_time,
		orders.deposit_location,
		orders.deposit_address,
		orders.deposit_app,
		orders.deposit_channel,
		orders.finger_print,
		refund.refund_amount as refund_amount, 	
		refund.apply_time as refund_time, 		
		refund.desc as redund_desc,	
		orders.pay_identify_code,
		orders.sn,
		orders.etc_ch_trade_no,
		orders.app_channel as appname,
		company.company_id, 
		orders.deposit_amount  as order_gmv_amount,
		'ETC' as bussiness_scope,
		'ETC充值' as bussiness_topic
from ods.etc_h077_deposit_order  orders
left join ods.etc_h077_deposit_order_extend  b on orders.order_id=b.order_id and b.dt = '${datekey}'
left join ods.etc_h077_apply_refund  refund on orders.order_id=refund.pre_order_id and refund.dt='${datekey}' and refund.status=2
left join dw_etc.dw_etc_company_deposit_order_new company on orders.order_id=company.order_no and company.dt='${datekey}'
left join dim.dim_field_enum_des_manual manual on cast(orders.status as string)=manual.field_key  and manual.table_name='etc_h077_deposit_order'and manual.dt='2019-04-24'
left join dwb.umd_user_base_info_s_d user_info on split(orders.operate_id, '_')[1]=user_info.user_id and user_info.dt='${datekey}'
where orders.dt='${datekey}' 
	and orders.app_channel=0 
	AND orders.pay_method != '三秦通转卡'
	and split(orders.operate_id, '_')[0]=1

union all

select 
		dim.mask(concat(orders.order_id,'ETC业务','ETC充值业务'), 'ENCRY') as trade_id,
		orders.order_id,
		orders.deposit_amount as order_amount,
		orders.deposit_time,
		manual.field_map_value as  order_status,
		manual.field_key as origin_status,
		orders.add_time as create_time,
		orders.modify_time as update_time,
		orders.deposit_amount as pay_amount_due,
		orders.deposit_amount as pay_amount,
		orders.channel,
		case when company.order_no is null then 0 else 1 end as deposit_type,
		orders.type,
		case when orders.channel in (1,2) then 'HCB'
			 when orders.channel = 3 then 'YMM' end as order_plateform,
		concat(case when orders.channel in (1,2) then 'HCB' when orders.channel = 3 then 'YMM' end,
				case when orders.app_channel = 0 then '管理版'  when orders.app_channel = 1 then '司机版' when orders.app_channel = 2 then '企业版' end) as order_channel,
		case when orders.pay_method like '%ABC%' then '中国农业银行'
			 when orders.pay_method like '%ALIPAY%' then '支付宝'
			 when orders.pay_method like '%BAITIAO%' then '白条支付'
			 when orders.pay_method like '%BCCB%' then '北京银行'
			 when orders.pay_method like '%BOC%' then '中国银行'
			 when orders.pay_method like '%CCB%' then '中国建设银行'
			 when orders.pay_method like '%CEB%' then '中国光大银行'
			 when orders.pay_method like '%CIB%' then '兴业银行'
			 when orders.pay_method like '%CITIC%' then '中信银行'
			 when orders.pay_method like '%CMB%' then '招商银行'
			 when orders.pay_method like '%CMBC%' then '中国民生银行'
			 when orders.pay_method like '%COMM%' then '中国交通银行'
			 when orders.pay_method like '%GDB%' then '广东发展银行'
			 when orders.pay_method like '%HXB%' then '华夏银行'
			 when orders.pay_method like '%ICBC%' then '中国工商银行'
			 when orders.pay_method like '%PAYECO%' then '易联支付'
			 when orders.pay_method like '%PSBC%' then '中国邮政储蓄银行'
			 when orders.pay_method like '%SDB%' then '深圳发展银行'
			 when orders.pay_method like '%SPDB%' then '上海浦东发展银行'
			 when orders.pay_method like '%SZPAB%' then '平安银行'
			 when orders.pay_method like '%UROVOPOS%' then '银联POS机'
			 when orders.pay_method like '%WXPAY%' then '微信支付'
			 when orders.pay_method like '%YEEPAY%' then '易宝支付'
			 when orders.pay_method like '%BALANCE%' then '钱包、余额'
			 when orders.pay_method like '%ETC_ACCOUNT%' then 'ETC专项金'
			 when orders.pay_method like '%BANKCARD_QPAY%' then '快捷支付'
			 when orders.pay_method like '%BANK_WEB%' then 'web页面支付'
			 when orders.pay_method like '%BOS%' then '上海银行'
			 when orders.pay_method like '%COUPON%' then '卡券支付'
			 else orders.pay_method end as pay_channel,
		orders.van_number,
		dim.mask(concat(orders.etc_card,'ETC业务','ETC卡'),'ENCRY') as product_id,
		user_info.user_id as operator_id,
		split(orders.operate_id,'_')[1] as operator_ymm_id,		
		
		if(to_date(orders.apply_time)='1900-01-01',orders.modify_time,orders.apply_time) as apply_time,
		orders.etc_card as etc_card_no,
		null  as card_name,
		 
		orders.blance as available_amount,
		orders.system_trace,
		orders.tac_nubmer,
		orders.card_bill,
		orders.trade_bill,
		orders.device_no,
		orders.device_bill,
		orders.pay_method,
		orders.pay_account,
		pay_time,
		orders.deposit_location,
		orders.deposit_address,
		orders.deposit_app,
		orders.deposit_channel,
		orders.finger_print,
		refund.refund_amount as refund_amount, 	
		refund.apply_time as refund_time, 		
		refund.desc as redund_desc,	
		orders.pay_identify_code,
		orders.sn,
		orders.etc_ch_trade_no,
		orders.app_channel as appname,
		company.company_id, 
		orders.deposit_amount  as order_gmv_amount,
		'ETC' as bussiness_scope,
		'ETC充值' as bussiness_topic
from ods.etc_h077_deposit_order  orders
left join ods.etc_h077_deposit_order_extend  b on orders.order_id=b.order_id and b.dt = '${datekey}'
left join ods.etc_h077_apply_refund  refund on orders.order_id=refund.pre_order_id and refund.dt='${datekey}' and refund.status=2
left join dw_etc.dw_etc_company_deposit_order_new company on orders.order_id=company.order_no and company.dt='${datekey}'
left join dim.dim_field_enum_des_manual manual on cast(orders.status as string)=manual.field_key  and manual.table_name='etc_h077_deposit_order'and manual.dt='2019-04-24'
left join dwb.umd_user_base_info_s_d user_info on split(orders.operate_id, '_')[1]=user_info.ymm_uid and user_info.dt='${datekey}'
where orders.dt='${datekey}' 
	and orders.app_channel=0 
	AND orders.pay_method != '三秦通转卡'
	and split(orders.operate_id, '_')[0]=99;


--充值成功但是不存在于notify中的数据为运营手动调整的订单,这些不上报第三方
--充值成功的基本都存在于notify
--第三方上报数据多1900, 则取第三方modify_time
INSERT OVERWRITE TABLE dwd.etc_deposit_order_fact_s_d PARTITION (dt)
SELECT trade_id		
		,deposit.order_id		
		,order_amount	
		,if(to_date(notify.info.notify_time)='1900-01-01',notify.info.modify_time,notify.info.notify_time) as deposit_time    
		,order_status	
		,origin_status
		,create_time		
		,update_time		
		,pay_amount_due	
		,pay_amount		
		,channel			
		,deposit_type    
		,type			
		,order_plateform	
		,order_channel	
		,pay_channel		
		,van_number		
		,product_id      
		,operator_id	
		,operator_ymm_id
		,apply_time		
		,etc_card_no
		,case   when substr(deposit.etc_card_no,1,4) ='1101' then '速通卡(北京)'
						when substr(deposit.etc_card_no,1,4) ='1201' then '速通卡(天津)'
						when substr(deposit.etc_card_no,1,4) ='1301' then '速通卡(河北)'
						when substr(deposit.etc_card_no,1,4) ='1401' then '快通卡'
						when substr(deposit.etc_card_no,1,4) ='1501' then '蒙通卡'
						when substr(deposit.etc_card_no,1,4) ='2101' then '辽通卡'
						when substr(deposit.etc_card_no,1,4) ='2201' then '吉通卡'
						when substr(deposit.etc_card_no,1,4) ='2301' then '黑通卡'
						when substr(deposit.etc_card_no,1,4) ='3101' then '沪通卡'
						when substr(deposit.etc_card_no,1,4) ='3201' and substr(etc_card_no,9,4)!='2210' then '苏通卡'
						when substr(deposit.etc_card_no,1,4) ='3201' and substr(etc_card_no,9,4) ='2210' then '苏通运政卡'
						when substr(deposit.etc_card_no,1,4) ='3301' then '浙通卡'
						when substr(deposit.etc_card_no,1,4) ='3401' then '皖通卡'
						when substr(deposit.etc_card_no,1,4) ='3501' then '闽通卡'
						when substr(deposit.etc_card_no,1,4) ='3601' then '赣通卡'
						when substr(deposit.etc_card_no,1,4) ='3701' then '鲁通卡'
						when substr(deposit.etc_card_no,1,4) ='3702' then '鲁通信联卡'
						when substr(deposit.etc_card_no,1,4) ='4101' then '中原通'
						when substr(deposit.etc_card_no,1,4) ='4201' then '通衢卡'
						when substr(deposit.etc_card_no,1,4) ='4301' then '湘通卡'
						when substr(deposit.etc_card_no,1,4) ='4401' then '粤通卡'
						when substr(deposit.etc_card_no,1,4) ='4501' then '八桂行'
						when substr(deposit.etc_card_no,1,4) ='5001' then '渝通卡'
						when substr(deposit.etc_card_no,1,4) ='5101' then '蜀通卡'
						when substr(deposit.etc_card_no,1,4) ='5201' then '黔通卡'
						when substr(deposit.etc_card_no,1,4) ='5301' then '云通卡'
						when substr(deposit.etc_card_no,1,4) ='6101' then '三秦通'
						when substr(deposit.etc_card_no,1,4) ='6201' then '陇通卡'
						when substr(deposit.etc_card_no,1,4) ='6301' then '青通卡'
						when substr(deposit.etc_card_no,1,4) ='6401' then '宁通卡'
						when substr(deposit.etc_card_no,1,4) ='6501' then '新通卡' 
					else '未知' end as card_name         
		,available_amount
		,system_trace	
		,tac_nubmer		
		,card_bill		
		,trade_bill		
		,device_no		
		,device_bill		
		,pay_method		
		,pay_account		
		,pay_time		
		,deposit_location
		,deposit_address	
		,deposit_app		
		,deposit_channel	
		,finger_print	
		,refund_amount	
		,refund_time		
		,refund_desc		
		,pay_identify_code
		,sn				
		,etc_ch_trade_no	
		,appname			
		,company_id      
		,order_gmv_amount
		,bussiness_scope	
		,bussiness_topic	
		,'${datekey}' dt 
FROM dwd.etc_deposit_order_fact_s_d  deposit
JOIN (
	SELECT order_id,
	        max(named_struct('notify_time',notify_time,'modify_time',modify_time)) info       --第三方上报的时间1900的数据特别多
	FROM ods.etc_h077_deposit_notify
	WHERE dt = '${datekey}'
	group by order_id
	) notify ON deposit.order_id = notify.order_id
WHERE deposit.dt = '${datekey}' 
	AND deposit.origin_status = 3
	
	
UNION ALL

SELECT   t.trade_id		
		,t.order_id		
		,t.order_amount	
		,t.deposit_time    
		,t.order_status	
		,t.origin_status
		,t.create_time		
		,t.update_time		
		,t.pay_amount_due	
		,t.pay_amount		
		,t.channel			
		,t.deposit_type    
		,t.type			
		,t.order_plateform	
		,t.order_channel	
		,t.pay_channel		
		,t.van_number		
		,t.product_id      
		,t.operator_id
		,t.operator_ymm_id
		,t.apply_time		
		,t.etc_card_no	
		,case   when substr(t.etc_card_no,1,4) ='1101' then '速通卡(北京)'
						when substr(t.etc_card_no,1,4) ='1201' then '速通卡(天津)'
						when substr(t.etc_card_no,1,4) ='1301' then '速通卡(河北)'
						when substr(t.etc_card_no,1,4) ='1401' then '快通卡'
						when substr(t.etc_card_no,1,4) ='1501' then '蒙通卡'
						when substr(t.etc_card_no,1,4) ='2101' then '辽通卡'
						when substr(t.etc_card_no,1,4) ='2201' then '吉通卡'
						when substr(t.etc_card_no,1,4) ='2301' then '黑通卡'
						when substr(t.etc_card_no,1,4) ='3101' then '沪通卡'
						when substr(t.etc_card_no,1,4) ='3201' and substr(etc_card_no,9,4)!='2210' then '苏通卡'
						when substr(t.etc_card_no,1,4) ='3201' and substr(etc_card_no,9,4) ='2210' then '苏通运政卡'
						when substr(t.etc_card_no,1,4) ='3301' then '浙通卡'
						when substr(t.etc_card_no,1,4) ='3401' then '皖通卡'
						when substr(t.etc_card_no,1,4) ='3501' then '闽通卡'
						when substr(t.etc_card_no,1,4) ='3601' then '赣通卡'
						when substr(t.etc_card_no,1,4) ='3701' then '鲁通卡'
						when substr(t.etc_card_no,1,4) ='3702' then '鲁通信联卡'
						when substr(t.etc_card_no,1,4) ='4101' then '中原通'
						when substr(t.etc_card_no,1,4) ='4201' then '通衢卡'
						when substr(t.etc_card_no,1,4) ='4301' then '湘通卡'
						when substr(t.etc_card_no,1,4) ='4401' then '粤通卡'
						when substr(t.etc_card_no,1,4) ='4501' then '八桂行'
						when substr(t.etc_card_no,1,4) ='5001' then '渝通卡'
						when substr(t.etc_card_no,1,4) ='5101' then '蜀通卡'
						when substr(t.etc_card_no,1,4) ='5201' then '黔通卡'
						when substr(t.etc_card_no,1,4) ='5301' then '云通卡'
						when substr(t.etc_card_no,1,4) ='6101' then '三秦通'
						when substr(t.etc_card_no,1,4) ='6201' then '陇通卡'
						when substr(t.etc_card_no,1,4) ='6301' then '青通卡'
						when substr(t.etc_card_no,1,4) ='6401' then '宁通卡'
						when substr(t.etc_card_no,1,4) ='6501' then '新通卡' 
					else '未知' end as card_name         
		,t.available_amount
		,t.system_trace	
		,t.tac_nubmer		
		,t.card_bill		
		,t.trade_bill		
		,t.device_no		
		,t.device_bill		
		,t.pay_method		
		,t.pay_account		
		,t.pay_time		
		,t.deposit_location
		,t.deposit_address	
		,t.deposit_app		
		,t.deposit_channel	
		,t.finger_print	
		,t.refund_amount	
		,t.refund_time		
		,t.refund_desc		
		,t.pay_identify_code
		,t.sn				
		,t.etc_ch_trade_no	
		,t.appname			
		,t.company_id      
		,t.order_gmv_amount
		,t.bussiness_scope	
		,t.bussiness_topic	
		,'${datekey}' dt
from dwd.etc_deposit_order_fact_s_d t
WHERE t.dt = '${datekey}'
AND t.origin_status <> 3;