
set hive.exec.dynamic.partition.mode=nonstrict;
create table if not exists dim.etc_card_dim_s_d(
product_id   string	comment '商品唯一ID',
etc_card_no  string comment 'ETC卡号',
product_name string comment 'ETC卡名',
card_type    string comment 'ETC卡类型(储值卡/记账卡)',
user_id      bigint comment '用户ID',
open_date    string comment '开卡日期',
source       string comment '卡来源',
van_number   string comment '车牌号',
van_plate_color int comment '车牌颜色',
province_name string comment '卡所属省',
type         string  comment '卡所属用户类型(个人/单位/未知)',
last_deposit_time string comment '最后一次充值时间',
company_org_id    string comment '卡最后一个绑定企业组织ID',
company_org_set string comment '卡绑定企业组织ID集合'
)comment 'ETC卡维度表'
partitioned by (dt string) 
stored as orc
TBLPROPERTIES("creator"="dongli.tan@56qq.com","safe_level"="C4","importance"="重要");



ALTER TABLE dim.etc_card_dim_s_d DROP IF EXISTS PARTITION(dt='${datekey}'); 
insert overwrite table dim.etc_card_dim_s_d partition(dt= '${datekey}')
select  dim.mask(concat(x.etc_card_no,'ETC业务','ETC卡'),'ENCRY') as product_id,
		x.etc_card_no,
		card_name,
		card_type,
		y.user_id,
		open_date,
		source,
		van_number,
		van_plate_color,
		case  when substr(x.etc_card_no,1,4) ='1101' then '北京'
		      when substr(x.etc_card_no,1,4) ='1201' then '天津'
		      when substr(x.etc_card_no,1,4) ='1301' then '河北'
		      when substr(x.etc_card_no,1,4) ='1401' then '山西'
		      when substr(x.etc_card_no,1,4) ='1501' then '内蒙古'
		      when substr(x.etc_card_no,1,4) ='2101' then '辽宁'
		      when substr(x.etc_card_no,1,4) ='2201' then '吉林'
		      when substr(x.etc_card_no,1,4) ='2301' then '黑龙江'
		      when substr(x.etc_card_no,1,4) ='3101' then '上海'
		      when substr(x.etc_card_no,1,4) ='3201' then '江苏'
		      when substr(x.etc_card_no,1,4) ='3301' then '浙江'
		      when substr(x.etc_card_no,1,4) ='3401' then '安徽'
		      when substr(x.etc_card_no,1,4) ='3501' then '福建'
		      when substr(x.etc_card_no,1,4) ='3601' then '江西'
		      when substr(x.etc_card_no,1,4) in ('3701','3702') then '山东'
		      when substr(x.etc_card_no,1,4) ='4101' then '河南'
		      when substr(x.etc_card_no,1,4) ='4201' then '湖北'
		      when substr(x.etc_card_no,1,4) ='4301' then '湖南'
		      when substr(x.etc_card_no,1,4) ='4401' then '广东'
		      when substr(x.etc_card_no,1,4) ='4501' then '广西'
		      when substr(x.etc_card_no,1,4) ='5001' then '重庆'
		      when substr(x.etc_card_no,1,4) ='5101' then '四川'
		      when substr(x.etc_card_no,1,4) ='5201' then '贵州'
		      when substr(x.etc_card_no,1,4) ='5301' then '云南'
		      when substr(x.etc_card_no,1,4) ='6101' then '陕西'
		      when substr(x.etc_card_no,1,4) ='6201' then '甘肃'
		      when substr(x.etc_card_no,1,4) ='6301' then '青海'
		      when substr(x.etc_card_no,1,4) ='6401' then '宁夏'
		      when substr(x.etc_card_no,1,4) ='6501' then '新疆' 
			  else null end as province_name,
		type,
		last_deposit_time,
		company_org_id,
		company_org_set
from(
		select 	etc_card_no
				,case   when substr(card.etc_card_no,1,4) ='1101' then '速通卡(北京)'
						when substr(card.etc_card_no,1,4) ='1201' then '速通卡(天津)'
						when substr(card.etc_card_no,1,4) ='1301' then '速通卡(河北)'
						when substr(card.etc_card_no,1,4) ='1401' then '快通卡'
						when substr(card.etc_card_no,1,4) ='1501' then '蒙通卡'
						when substr(card.etc_card_no,1,4) ='2101' then '辽通卡'
						when substr(card.etc_card_no,1,4) ='2201' then '吉通卡'
						when substr(card.etc_card_no,1,4) ='2301' then '黑通卡'
						when substr(card.etc_card_no,1,4) ='3101' then '沪通卡'
						when substr(card.etc_card_no,1,4) ='3201' and substr(etc_card_no,9,4)!='2210' then '苏通卡'
						when substr(card.etc_card_no,1,4) ='3201' and substr(etc_card_no,9,4) ='2210' then '苏通运政卡'
						when substr(card.etc_card_no,1,4) ='3301' and card_type=22 then '浙通卡'
						when substr(card.etc_card_no,1,4) ='3401' then '皖通卡'
						when substr(card.etc_card_no,1,4) ='3501' then '闽通卡'
						when substr(card.etc_card_no,1,4) ='3601' then '赣通卡'
						when substr(card.etc_card_no,1,4) ='3701' and card_type=22 then '鲁通卡'
						when substr(card.etc_card_no,1,4) ='3702' then '鲁通信联卡'
						when substr(card.etc_card_no,1,4) ='4101' then '中原通'
						when substr(card.etc_card_no,1,4) ='4201' then '通衢卡'
						when substr(card.etc_card_no,1,4) ='4301' then '湘通卡'
						when substr(card.etc_card_no,1,4) ='4401' then '粤通卡'
						when substr(card.etc_card_no,1,4) ='4501' then '八桂行'
						when substr(card.etc_card_no,1,4) ='5001' and card_type=22 then '渝通卡'
						when substr(card.etc_card_no,1,4) ='5101' then '蜀通卡'
						when substr(card.etc_card_no,1,4) ='5201' then '黔通卡'
						when substr(card.etc_card_no,1,4) ='5301' then '云通卡'
						when substr(card.etc_card_no,1,4) ='6101' then '三秦通'
						when substr(card.etc_card_no,1,4) ='6201' then '陇通卡'
						when substr(card.etc_card_no,1,4) ='6301' then '青通卡'
						when substr(card.etc_card_no,1,4) ='6401' then '宁通卡'
						when substr(card.etc_card_no,1,4) ='6501' then '新通卡' 
						when substr(card.etc_card_no,1,4) ='3701' and card_type=23 then '齐鲁记账卡'
						when substr(card.etc_card_no,1,4) ='5001' and card_type=23 then '千方记账卡'
						when substr(card.etc_card_no,1,4) ='3301' and card_type=23 then '浙江记账卡'
					else '未知' end as card_name
				,open_date
				,source
				,van_number
				,van_plate_color
				,case when type=0 then '个人'
						when type=1 then '单位'
						else '未知' end as type
				,case when card_type=22 then '储值卡'
						when card_type=23 then '记账卡'
						when card_type=0 then '储值卡'
						end as card_type
				,last_deposit_time
		-----开卡表中的ETC卡
		from(
				select  open.card_face_nubmer as etc_card_no
						,to_date(open.card_info.add_time)  as open_date
						,'open_card_order' as source
						,open.card_info.van_number
						,open.card_info.van_plate_color
						,open.card_info.type 
						,open.card_info.card_type               ---0:个人 1:企业
						,deposit.last_deposit_time
				from (
					 select card_face_nubmer,
							max(named_struct('add_time',add_time,'van_number',van_number,'van_plate_color',van_plate_color,'type',type,'card_type',card_type)) as card_info
						from ods.etc_h077_open_card_order 
							where dt='${datekey}'
							and STATUS = 1
							group by card_face_nubmer
					) open
				left join(
						SELECT etc_card,
								max(deposit_time) as last_deposit_time
						FROM ods.etc_h077_deposit_order  
						WHERE dt='${datekey}' 
						group by etc_card
					)deposit on open.card_face_nubmer=deposit.etc_card
				
				union all
				
				select 	deposit.etc_card as etc_card_no
						,null as open_date
						,'deposit_order' as source
						,deposit.deposit_info.van_number
						,null as van_plate_color
						,2 as type                 ---未知
						,22 as card_type
						,deposit.deposit_info.deposit_time as last_deposit_time
				from(
					SELECT etc_card,
							max(named_struct('deposit_time',deposit_time,'van_number',van_number)) as deposit_info
					FROM ods.etc_h077_deposit_order  
					WHERE dt='${datekey}' 
					group by etc_card
					)deposit
				left join(
						select a.card_face_nubmer as etc_card_no
						from  ods.etc_h077_open_card_order a
						where a.dt='${datekey}'
						and a.STATUS = 1
						group by a.card_face_nubmer
					)open on deposit.etc_card=open.etc_card_no
				where open.etc_card_no is null 
			)card
			
		union all
			
		select credit_card.etc_card as  etc_card_no
				,case when credit_card.credit_channel= 0   then   '千方记账卡'
						when credit_card.credit_channel= 1   then '齐鲁记账卡'
						when credit_card.credit_channel= 2   then '浙江记账卡'
					end as card_name
				,credit_card.open_date
				,'credit_car' as source
				,credit_card.van_number
				,credit_card.van_plate_color
				,case when credit_card.user_type=1 then '个人'
						when credit_card.user_type=0 then '单位'
					else '未知' end as type	             ---2企业 1个人 
				,'记账卡' as card_type
				,null as last_deposit_time
		from(
				select etc_card,
						credit_channel,
						to_date(open.add_time) as open_date,
						van_number,
						van_plate_color,
						user_type
				from  ods.etc_h080_credit_car_info open
				where dt = '${datekey}'
					and length(etc_card)> 0
					and status=3
				group by  etc_card,
						credit_channel,
						to_date(open.add_time),
						van_number,
						van_plate_color,
						user_type
			)credit_card
		left join(
				select a.card_face_nubmer as etc_card
				from ods.etc_h077_open_card_order a
				where a.dt='${datekey}'
					and a.STATUS = 1
				group by a.card_face_nubmer
			)open_card on credit_card.etc_card=open_card.etc_card
		where open_card.etc_card is null
	)x
left join  dim.etc_user_dim_by_card_s_d y on x.etc_card_no=y.etc_card_no and y.dt='${datekey}'
left join(
	select card_face_nubmer,
			company_info.user_id as company_org_id,
			company_org_set
		from (
			select 
				card_face_nubmer,
				concat_ws('|',collect_set(user_id)) as company_org_set,
				max(named_struct('add_time',add_time,'user_id',user_id)) as company_info
			from  ods.etc_h077_user_card_record a 
			where dt='${datekey}'
				and domain_id=999
				and status=0
			group by card_face_nubmer
			) t
	)z on x.etc_card_no=z.card_face_nubmer;