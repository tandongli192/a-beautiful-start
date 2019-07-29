set hive.exec.dynamic.partition.mode=nonstrict;


create table if not exists dwd.etc_employee_dim_s_d (
proxy_user_id STRING COMMENT '代理员工user_id',
proxy_acct STRING COMMENT '代理账号',
proxy_name STRING COMMENT '代理姓名',
proxy_ymm_work_no STRING COMMENT '代理员工ymm员工工号',

emp_user_id STRING COMMENT'内部员工user_id',
emp_name  string comment '员工姓名',
emp_work_no STRING COMMENT '内部员工工号',
organization_id int comment '员工组织id',
emp_region  string comment '员工所属大区',
emp_area  string comment '员工所属片区',
emp_department  STRING COMMENT '员工组织架构所属部门所属渠道',
emp_city   string comment '员工所属组织架构城市',
emp_city_id string comment '员工所属组织架构城市id',
emp_province string comment '员工所属组织架构省份',
emp_province_id string comment '员工所属组织架构省份id',
emp_belong INT COMMENT '员工所属(1HCB内部员工/2HCB代理员工/3运满满员工)',
emp_perf_belong STRING COMMENT '员工业绩所属(三大区/呼叫中心/运满满/HCB代理/大客户部/其他)',
emp_type STRING COMMENT '员工类型(司机类/货主类)'
)COMMENT 'ETc员工维度表' 
partitioned by (dt string) stored as orc;


ALTER TABLE dwd.etc_employee_dim_s_d DROP IF EXISTS PARTITION(dt='${datekey}'); 
insert overwrite table dwd.etc_employee_dim_s_d partition(dt= '${datekey}')
select proxy_user_id
	  ,proxy_acct
	  ,proxy_name
	  ,proxy_ymm_work_no 
	  ,emp_user_id
	  ,emp_name
	  ,work_no emp_work_no
	  ,x.organization_id
	  ,emp_region
	  ,emp_area
	  ,emp_department
	  ,emp_city
	  ,c.city_id as emp_city_id
	  ,nvl(y.province_name,region.emp_province) emp_province
	  ,d.province_id as  emp_province_id
	  
	  ---此处修改由于产品线上改变了YMM员工的用户为内部用户 并且将用户名改成了YMM开头的
	  ---统计YMM业绩时 只统计 2018-04-17之后的
	  ,case when source is null then 1
			when proxy_ymm_work_no is null and source is not null then 2
			when proxy_ymm_work_no is not null then 3
	   end emp_belong
	  ,case when proxy_ymm_work_no is null and source is not null then 'HCB代理' 
	  	    when proxy_ymm_work_no is not null then '运满满'
	  	    when emp_department = '呼叫中心' then '呼叫中心'
	  	    when emp_department = '大客户部' then '大客户部'
	  	    when emp_department = '销售部' and emp_region in ('中部大区','北部大区','南部大区') then emp_region
	  	    else '其他'
	   end emp_perf_belong
	   ,z.name emp_type
from (
	---内部员工  只有代理id有值,其他代理信息为空
	SELECT user_id proxy_user_id,
			user_id emp_user_id,
			name as emp_name,
			work_no,
			null source,
			null proxy_ymm_work_no,
			null proxy_acct,
			null proxy_name,
			organization_id
	FROM  ods.pub_h001_t_employee
	WHERE dt = '${datekey}'
		
	
	UNION ALL 
	
	---真正的代理,针对于user_id 层级的
	SELECT split(x.son_id,'_')[1] proxy_user_id,
			employee.user_id emp_user_id,
			employee.name as emp_name,
			employee.work_no,
			source,
			case when source = 3 then x.son end proxy_ymm_work_no,
			son proxy_acct,
			son_name proxy_name,
			employee.organization_id
	FROM (
		select 	father_id,
				son_id,
				son,
				source,
				son_name
		from ods.etc_h081_etc_proxy 
		WHERE dt = '${datekey}'
		  and son_id is not null
		  and son_id != ''
		)x
	LEFT JOIN ods.pub_h001_t_employee employee on x.father_id = concat('1_',employee.user_id) and employee.dt = '${datekey}'
)x
left join(	
		SELECT l6_id as organization_id
			  ,regexp_extract(l3_name,'(^.+?)(省|市|壮族自治区|回族自治区|维吾尔自治区|自治区|特别行政区)',1) emp_province
			  ,l5_name emp_city
			  ,l2_name as emp_region
			  ,l4_name as emp_area
			  ,l1_name as emp_department
		FROM logisticsqqdb.dwd_organization_new
		WHERE dt = '${datekey}'
) region ON x.organization_id = region.organization_id
LEFT JOIN(
	select city_name
		  ,province_name 
	from default.hive_region_dim 
	group by city_name
		    ,province_name
	)y on region.emp_city = y.city_name
left join (
	select city_id
	      ,city_name
	from default.hive_region_dim 
	where dt = '${datekey}'
		and length(city_id) > 2
	group by  city_id
	      ,city_name
	)c on region.emp_city = c.city_name
left join (
	select province_id
	      ,province_name
	from default.hive_region_dim 
	where dt = '${datekey}'
	group by province_id
	      ,province_name
	)d on region.emp_province = d.province_name	
left join (
	select b.user_id
		  ,c.name
	from ods.ms2_h051_user_label  b
	join(
		select * 
		from ods.ms2_h051_label 
		where dt='${datekey}' 
			and plan_id=4 
			and status=1 
		---and name in ('司机类','货主类')
		) c on b.label_id=c.id
	where b.dt='${datekey}' 
		and b.status=1
) z on x.proxy_user_id = z.user_id;


