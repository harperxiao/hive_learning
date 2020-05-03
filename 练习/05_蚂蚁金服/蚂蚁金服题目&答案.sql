背景说明：
以下表记录了用户每天的蚂蚁森林低碳生活领取的记录流水。
table_name：user_low_carbon
user_id data_dt  low_carbon
用户     日期      减少碳排放（g）

蚂蚁森林植物换购表，用于记录申领环保植物所需要减少的碳排放量
table_name:  plant_carbon
plant_id plant_name low_carbon
植物编号	植物名	换购植物所需要的碳

----题目
1.蚂蚁森林植物申领统计
问题：假设2017年1月1日开始记录低碳数据（user_low_carbon），假设2017年10月1日之前满足申领条件的用户都申领了一颗p004-胡杨，
剩余的能量全部用来领取“p002-沙柳” 。
统计在10月1日累计申领“p002-沙柳” 排名前10的用户信息；以及他比后一名多领了几颗沙柳。
得到的统计结果如下表样式：
user_id  plant_count less_count(比后一名多领了几颗沙柳)
u_101    1000         100
u_088    900          400
u_103    500          …


2、蚂蚁森林低碳用户排名分析
问题：查询user_low_carbon表中每日流水记录，条件为：
用户在2017年，连续三天（或以上）的天数里，
每天减少碳排放（low_carbon）都超过100g的用户低碳流水。
需要查询返回满足以上条件的user_low_carbon表中的记录流水。
例如用户u_002符合条件的记录如下，因为2017/1/2~2017/1/5连续四天的碳排放量之和都大于等于100g：
seq（key） user_id data_dt  low_carbon
xxxxx10    u_002  2017/1/2  150
xxxxx11    u_002  2017/1/2  70
xxxxx12    u_002  2017/1/3  30
xxxxx13    u_002  2017/1/3  80
xxxxx14    u_002  2017/1/4  150
xxxxx14    u_002  2017/1/5  101
备注：统计方法不限于sql、procedure、python,java等


提供的数据说明：
user_low_carbon：
u_001	2017/1/1	10
u_001	2017/1/2	150
u_001	2017/1/2	110
u_001	2017/1/2	10
u_001	2017/1/4	50
u_001	2017/1/4	10
u_001	2017/1/6	45
u_001	2017/1/6	90
u_002	2017/1/1	10
u_002	2017/1/2	150
u_002	2017/1/2	70
u_002	2017/1/3	30
u_002	2017/1/3	80
u_002	2017/1/4	150
u_002	2017/1/5	101
u_002	2017/1/6	68
...

plant_carbon：
p001	梭梭树	17
p002	沙柳	19
p003	樟子树	146
p004	胡杨	215
...

1.创建表
create table user_low_carbon(user_id String,data_dt String,low_carbon int) row format delimited fields terminated by '\t';
create table plant_carbon(plant_id string,plant_name String,low_carbon int) row format delimited fields terminated by '\t';

2.加载数据
load data local inpath "/opt/module/data/user_low_carbon.txt" into table user_low_carbon;
load data local inpath "/opt/module/data/plant_carbon.txt" into table plant_carbon;

3.设置本地模式
set hive.exec.mode.local.auto=true;

一：
1.统计每个用户截止到2017/10/1日期总低碳量
select
    user_id,
    sum(low_carbon) sum_low_carbon
from
    user_low_carbon
where
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM')<'2017-10'
group by
    user_id
order by
    sum_low_carbon desc
limit 11;t1

2.取出胡杨的能量
select low_carbon from plant_carbon where plant_id='p004';t2

3.取出沙柳的能量
select low_carbon from plant_carbon where plant_id='p002';t3

4.计算每个人申领沙柳的棵数
select
    user_id,
    floor((sum_low_carbon-t2.low_carbon)/t3.low_carbon) plant_count
from
    t1,t2,t3;t4

5.按照申领沙柳棵数排序,并将下一行数据中的plant_count放置当前行
select
    user_id,
    plant_count,
    lead(plant_count,1,'9999-99-99') over(order by plant_count desc) lead_plant_count
from
    t4
limit 10;t5

6.求相差的沙柳棵数
select
    user_id,
    plant_count,
    (plant_count-lead_plant_count) plant_count_diff
from
    t5;

select
    user_id,
    plant_count,
    lead(plant_count,1,'9999-99-99') over(order by plant_count desc) lead_plant_count
from
    (select
    user_id,
    floor((sum_low_carbon-t2.low_carbon)/t3.low_carbon) plant_count
from
    (select
    user_id,
    sum(low_carbon) sum_low_carbon
from
    user_low_carbon
where
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM')<'2017-10'
group by
    user_id)t1,
    (select low_carbon from plant_carbon where plant_id='p004')t2,
    (select low_carbon from plant_carbon where plant_id='p002')t3)t4
limit 10;t5


select
    user_id,
    plant_count,
    (plant_count-lead_plant_count) plant_count_diff
from
    (select
    user_id,
    plant_count,
    lead(plant_count,1,'9999-99-99') over(order by plant_count desc) lead_plant_count
from
    (select
    user_id,
    floor((sum_low_carbon-t2.low_carbon)/t3.low_carbon) plant_count
from
    (select
    user_id,
    sum(low_carbon) sum_low_carbon
from
    user_low_carbon
where
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM')<'2017-10'
group by
    user_id)t1,
    (select low_carbon from plant_carbon where plant_id='p004')t2,
    (select low_carbon from plant_carbon where plant_id='p002')t3)t4
order by
    plant_count desc
limit 10)t5;



二：
1.过滤出2017年且单日低碳量超过100g
select
    user_id,
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt
from
    user_low_carbon
where
    substring(data_dt,1,4)='2017'
group by
    user_id,data_dt
having
    sum(low_carbon)>=100;t1

2.将前两行数据以及后两行数据的日期放置当前行
select
    user_id,
    data_dt,
    lag(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lag2,
    lag(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lag1,
    lead(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lead1,
    lead(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lead2
from
    t1;

3.计算当前日期跟前后两行时间的差值
select
    user_id,
    data_dt,
    datediff(data_dt,lag2) lag2_diff,
    datediff(data_dt,lag1) lag1_diff,
    datediff(data_dt,lead1) lead1_diff,
    datediff(data_dt,lead2) lead2_diff
from
    t2;t3

4.过滤出连续3天超过100g的用户
select
    user_id,
    data_dt
from
    t3
where
    (lag2_diff=2 and lag1_diff=1) 
    or 
    (lag1_diff=1 and lead1_diff=-1) 
    or 
    (lead1_diff=-1 and lead2_diff=-2);t4

5.关联原表
select
    user.user_id,
    user.data_dt,
    user.low_carbon
from
    t4
join
    user_low_carbon user
on
    t4.user_id = user.user_id and t4.data_dt = date_format(regexp_replace(user.data_dt,'/','-'),'yyyy-MM-dd');


user_id data_dt
u_002   2017-01-02
u_002   2017-01-03
u_002   2017-01-04
u_002   2017-01-05
u_005   2017-01-02
u_005   2017-01-03
u_005   2017-01-04
u_008   2017-01-04
u_008   2017-01-05
u_008   2017-01-06
u_008   2017-01-07
u_009   2017-01-02
u_009   2017-01-03
u_009   2017-01-04
u_010   2017-01-04
u_010   2017-01-05
u_010   2017-01-06
u_010   2017-01-07
u_011   2017-01-01
u_011   2017-01-02
u_011   2017-01-03
u_011   2017-01-04
u_011   2017-01-05
u_011   2017-01-06
u_011   2017-01-07
u_013   2017-01-02
u_013   2017-01-03
u_013   2017-01-04
u_013   2017-01-05
u_014   2017-01-05
u_014   2017-01-06
u_014   2017-01-07

解法二：

2017/1/2 1 1-1
2017/1/3 2 1-1
2017/1/4 3 1-1
2017/1/5 4 1-1
2017/1/6 5 1-1
2017/1/8 6 1-2
2017/1/9 7 1-2

1.过滤出2017年且单日低碳量超过100g
select
    user_id,
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt
from
    user_low_carbon
where
    substring(data_dt,1,4)='2017'
group by
    user_id,data_dt
having
    sum(low_carbon)>=100;t1

2.按照日期进行排序，并给每一条数据一个标记
select
    user_id,
    data_dt,
    rank() over(partition by user_id order by data_dt) rk
from
    (select
    user_id,
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt
from
    user_low_carbon
where
    substring(data_dt,1,4)='2017'
group by
    user_id,data_dt
having
    sum(low_carbon)>=100)t1;t2

3.将日期减去当前的rank值
select
    user_id,
    data_dt,
    date_sub(data_dt,rk) data_sub_rk
from
    (select
    user_id,
    data_dt,
    rank() over(partition by user_id order by data_dt) rk
from
    (select
    user_id,
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt
from
    user_low_carbon
where
    substring(data_dt,1,4)='2017'
group by
    user_id,data_dt
having
    sum(low_carbon)>=100)t1)t2;t3

4.过滤出连续3天超过100g的用户
select
    user_id
from
    (select
    user_id,
    data_dt,
    date_sub(data_dt,rk) data_sub_rk
from
    (select
    user_id,
    data_dt,
    rank() over(partition by user_id order by data_dt) rk
from
    (select
    user_id,
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt
from
    user_low_carbon
where
    substring(data_dt,1,4)='2017'
group by
    user_id,data_dt
having
    sum(low_carbon)>=100)t1)t2)t3
group by 
    user_id,data_sub_rk
having
    count(*)>=10;

select
    user_id,
    data_dt,
    lag(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lag2,
    lag(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lag1,
    lead(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lead1,
    lead(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lead2
from
    (select
    user_id,
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt
from
    user_low_carbon
where
    substring(data_dt,1,4)='2017'
group by
    user_id,data_dt
having
    sum(low_carbon)>=100)t1;t2



select
    user_id,
    data_dt,
    datediff(data_dt,lag2) lag2_diff,
    datediff(data_dt,lag1) lag1_diff,
    datediff(data_dt,lead1) lead1_diff,
    datediff(data_dt,lead2) lead2_diff
from
    (select
    user_id,
    data_dt,
    lag(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lag2,
    lag(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lag1,
    lead(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lead1,
    lead(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lead2
from
    (select
    user_id,
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt
from
    user_low_carbon
where
    substring(data_dt,1,4)='2017'
group by
    user_id,data_dt
having
    sum(low_carbon)>=100)t1)t2;t3


select
    user_id,
    data_dt
from
    (select
    user_id,
    data_dt,
    datediff(data_dt,lag2) lag2_diff,
    datediff(data_dt,lag1) lag1_diff,
    datediff(data_dt,lead1) lead1_diff,
    datediff(data_dt,lead2) lead2_diff
from
    (select
    user_id,
    data_dt,
    lag(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lag2,
    lag(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lag1,
    lead(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lead1,
    lead(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lead2
from
    (select
    user_id,
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt
from
    user_low_carbon
where
    substring(data_dt,1,4)='2017'
group by
    user_id,data_dt
having
    sum(low_carbon)>=100)t1)t2)t3
where
    (lag2_diff=2 and lag1_diff=1) 
    or 
    (lag1_diff=1 and lead1_diff=-1) 
    or 
    (lead1_diff=-1 and lead2_diff=-2);t4


select
    user.user_id,
    user.data_dt,
    user.low_carbon
from
    (select
    user_id,
    data_dt
from
    (select
    user_id,
    data_dt,
    datediff(data_dt,lag2) lag2_diff,
    datediff(data_dt,lag1) lag1_diff,
    datediff(data_dt,lead1) lead1_diff,
    datediff(data_dt,lead2) lead2_diff
from
    (select
    user_id,
    data_dt,
    lag(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lag2,
    lag(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lag1,
    lead(data_dt,1,'1970-01-01') over(partition by user_id order by data_dt) lead1,
    lead(data_dt,2,'1970-01-01') over(partition by user_id order by data_dt) lead2
from
    (select
    user_id,
    date_format(regexp_replace(data_dt,'/','-'),'yyyy-MM-dd') data_dt
from
    user_low_carbon
where
    substring(data_dt,1,4)='2017'
group by
    user_id,data_dt
having
    sum(low_carbon)>=100)t1)t2)t3
where
    (lag2_diff=2 and lag1_diff=1) 
    or 
    (lag1_diff=1 and lead1_diff=-1) 
    or 
    (lead1_diff=-1 and lead2_diff=-2))t4
join
    user_low_carbon user
on
    t4.user_id = user.user_id and t4.data_dt = date_format(regexp_replace(user.data_dt,'/','-'),'yyyy-MM-dd');




2017/1/2 1970/01/01 1970/01/01 2017/1/3 2017/1/4
2017/1/3 1970/01/01 2017/1/2   2017/1/4 2017/1/6
2017/1/4
2017/1/6
2017/1/8
2017/1/9

a,b,c,d

(a=2  and b=1) or
(b=1  and c=-1) or
(c=-1 and d=-2)



MR:
mapper(key:user+date,value:一行)
grouping:user
reduce()


values:
{
	date = 1970-01-01
	list = new ArrayList()

	values.for{
		if(date==1970-01-01){//第一条数据
			list.add(value);
			date=value.dt;
		}else{
			if(value.dt-dt==1){
				list.add(value);
				date=value.dt;
			}else{
				if(list.size>=3){
				 context.wirte();
				 list.clear;
				 list.add(value);
				 date=value.dt
				}else{
				 list.clear;
				 list.add(value);
				 date=value.dt
				}
			}
		}
	}


		if(list.size>=3){
			context.wirte();
		}

}
