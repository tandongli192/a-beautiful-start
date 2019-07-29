
set hive.exec.dynamic.partition.mode=nonstrict;

create table if not exists dwd.etc_open_order_fact_s_d(
id				int		comment	'主键id',
order_id		string	comment	'开卡订单号',
user_name		string	comment	'办卡人账号',
open_user_id string	comment	'开卡用户id',
open_ymm_id  string comment '开卡用户ymmID',
open_type		int		comment	'开户类型(0-个人/1-单位)',
card_user_name	string	comment	'姓名',
open_ic_type	int	comment	'开卡证件类型(1-身份证/2-军官证/3-其他)',
open_ic_no		string	comment	'开卡证件号',
open_loc_city   string  comment '开卡城市',
open_platform 	string  comment '开卡平台(HCB/YMM/NONE)',
company_type	int		comment	'开卡用户是企业时的单位类型(-1个人/0-政府机关/1-事业单位/2-国有企业/3-外资企业/4-民营企业/5-其他)',
level			int		comment	'客户等级(1-VIP/0-普通)',
address			string	comment	'联系地址',
open_phone 		string  comment '开卡人预留手机号',
etc_card_no	  	string	comment	'卡片表面编号',
card_type		string	comment	'卡类型',
card_version	string	comment	'卡版本',
van_number		string	comment	'车牌号',
van_plate_color	int		comment	'车牌颜色(0-蓝/1-黄/2-黑/3-白)',
sex				int		comment	'持卡人性别(1-男/0-女)',
nation			string	comment	'持卡人民族',
contractor		string	comment	'联系人',
card_name		string	comment	'卡名字',
company			string	comment	'开卡单位',
native_place	string	comment	'持卡人籍贯',
status			int		comment	'生成订单状态(0-新订单/1-开卡成功/2-开卡失败)',
open_time		string	comment	'开卡时间',
modify_time		string	comment	'时间戳',
company_phone	string	comment	'企业唯一性手机号',

company_org_id  string  comment '企业org_id', 
bind_company_status INT comment '绑定企业状态(0未绑定企业/1为有营业执照企业/2为车队企业)',
bind_company_list 	string comment '绑定企业账号',
is_company_open 	INT comment '是否企业统一开卡(1是/0否)'

)COMMENT 'ETC开卡订单事实表' 
partitioned by (dt string) 
stored as orc
TBLPROPERTIES("creator"="zhiyuan.xu@56qq.com","safe_level"="C4","importance"="重要");

drop table if exists temporarydb.tmp_dwd_etc_core_card_info_01_20991231;
create table if not exists temporarydb.tmp_dwd_etc_core_card_info_01_20991231 as 
			  --此处修改由于产品线上改变了YMM员工的用户为内部用户 并且将用户名改成了YMM开头的
			  --统计YMM业绩时 只统计 20180417之后的
SELECT open.id				
		       ,open.order_id		
		       ,open.user_name		
		       ,open.user_id as open_user_id
			   ,b.ymm_uid as open_ymm_id
		       ,open.type as open_type		
		       ,open.card_user_name	
		       ,open.credential_type as open_ic_type	
		       ,open.credential_no   as open_ic_no		
		       ,case when regexp_extract(regexp_replace(OPEN.open_address,'中国',''), '(.*?)(省|回族自治区|维吾尔自治区|壮族自治区|自治区)(.*?)市', 3)  = '' 
				    	then regexp_extract(regexp_replace(OPEN.open_address,'中国',''), '(.*?)市', 1)
					else regexp_extract(regexp_replace(OPEN.open_address,'中国',''), '(.*?)(省|回族自治区|维吾尔自治区|壮族自治区|自治区)(.*?)市', 3)
				end  as open_loc_city   
		       ,case when all_user.username like '%YMM%' and to_date(open.add_time) >= '2018-04-17' then 'YMM' else 'HCB' end open_platform
		       ,open.company_type	
		       ,open.level			
		       ,open.address			
		       ,open.phone as open_phone 		
		       ,open.card_face_nubmer as etc_card_no	  	
		       ,open.card_type		
		       ,open.card_version	
		       ,open.van_number		
		       ,open.van_plate_color	
		       ,open.sex				
		       ,open.nation			
		       ,open.contractor		
		       ,card.product_name as card_name		
		       ,open.company			
		       ,open.native_place	
		       ,open.status			
		       ,open.add_time as open_time		
		       ,open.modify_time		
		       ,open.company_phone	
		   	  
FROM ods.etc_h077_open_card_order OPEN
left JOIN  DEFAULT.dwd_logisticsqq_all_users_full all_user ON OPEN.user_id = all_user.id and all_user.TYPE IN (12,15) AND all_user.dt = '${datekey}' 
left join dim.etc_card_dim_s_d card on OPEN.card_face_nubmer=card.etc_card_no and card.dt='${datekey}'
left join dwb.umd_user_base_info_s_d b on split(OPEN.user_id, '_')[1]=b.user_id and b.dt='${datekey}'
WHERE OPEN.STATUS = 1
	AND OPEN.dt = '${datekey}'
	AND to_date(OPEN.add_time) <= '${datekey}'
	and split(OPEN.user_id, '_')[0]=1
	
union all

SELECT open.id				
		       ,open.order_id		
		       ,open.user_name	
			   ,b.user_id as  open_user_id
		       ,OPEN.user_id as open_ymm_id

		       ,open.type as open_type		
		       ,open.card_user_name	
		       ,open.credential_type as open_ic_type	
		       ,open.credential_no   as open_ic_no		
		       ,case when regexp_extract(regexp_replace(OPEN.open_address,'中国',''), '(.*?)(省|回族自治区|维吾尔自治区|壮族自治区|自治区)(.*?)市', 3)  = '' 
				    	then regexp_extract(regexp_replace(OPEN.open_address,'中国',''), '(.*?)市', 1)
					else regexp_extract(regexp_replace(OPEN.open_address,'中国',''), '(.*?)(省|回族自治区|维吾尔自治区|壮族自治区|自治区)(.*?)市', 3)
				end  as open_loc_city   
		       ,case when all_user.username like '%YMM%' and to_date(open.add_time) >= '2018-04-17' then 'YMM' else 'HCB' end open_platform
		       ,open.company_type	
		       ,open.level			
		       ,open.address			
		       ,open.phone as open_phone 		
		       ,open.card_face_nubmer as etc_card_no	  	
		       ,open.card_type		
		       ,open.card_version	
		       ,open.van_number		
		       ,open.van_plate_color	
		       ,open.sex				
		       ,open.nation			
		       ,open.contractor		
		       ,card.product_name as card_name		
		       ,open.company			
		       ,open.native_place	
		       ,open.status			
		       ,open.add_time as open_time		
		       ,open.modify_time		
		       ,open.company_phone	
		   	  
FROM ods.etc_h077_open_card_order OPEN
left JOIN  DEFAULT.dwd_logisticsqq_all_users_full all_user ON OPEN.user_id = all_user.id and all_user.TYPE IN (12,15) AND all_user.dt = '${datekey}' 
left join dim.etc_card_dim_s_d card on OPEN.card_face_nubmer=card.etc_card_no and card.dt='${datekey}'
left join dwb.umd_user_base_info_s_d b on split(OPEN.user_id, '_')[1]=b.ymm_uid and b.dt='${datekey}'
WHERE OPEN.STATUS = 1
	AND OPEN.dt = '${datekey}'
	AND to_date(OPEN.add_time) <= '${datekey}'
	and split(OPEN.user_id, '_')[0]=99;			  



	
ALTER TABLE dwd.etc_open_order_fact_s_d DROP IF EXISTS PARTITION(dt='${datekey}'); 
insert overwrite table dwd.etc_open_order_fact_s_d partition(dt= '${datekey}')

	select x.*
		  ,bind.company_org as company_org_id
		  ,case when bind.bind_type_list = 1 then 1
		  		when bind.bind_type_list = 0 then 2
		  		else 0   end bind_company_status
		  ,case when bind.bind_type_list = 1 then bind.bind_company_list end bind_company_list
		  ,case when bind.etc_card is not null then 1 else 0 end is_company_open 
	from temporarydb.tmp_dwd_etc_core_card_info_01_20991231 x 
	left join(
		
		--有营业执照的企业批量开卡的叫企业卡
		select a.etc_card
			  ,a.company_org
			  ,concat_ws('|',collect_set(a.company_org)) 			bind_company_list
			  ,concat_ws('|',collect_set(cast(b.type as string))) 	bind_type_list
		from(
		    SELECT etc_card,
					company_org,
					add_time 
		    FROM ods.etc_h080_company_open_card  
		    where dt='${datekey}'
		    and status=3
		    and company_name not like '%测试%'
		) a
		join(
		    select company_org 
		    	  ,type
		    from ods.etc_h080_company_info 
		    where dt='${datekey}' 
		) b on a.company_org=b.company_org
		group by a.etc_card,
				a.company_org

	)bind on x.etc_card_no = bind.etc_card;