    
gulivideo_orc:
    videoId string, 
    uploader string, 
    age int, 
    category array<string>, 
    length int, 
    views int, 
    rate float, 
    ratings int, 
    comments int,
    relatedId array<string>

gulivideo_user_orc:
    uploader string,
    videos int,
    friends int



--统计视频观看数Top10
select
    videoId,
    views
from
    gulivideo_orc
order by
    views desc
limit 10;

dMH0bHeiRNg     42513417
0XxI-hvPRRA     20282464
1dmVU08zVpA     16087899
RB-wUgnyGv0     15712924
QjA5faZF1A8     15256922
-_CSo1gOd48     13199833
49IDp76kjPw     11970018
tYnn51C3X_w     11823701
pv5zWaTEVkI     11672017
D2kJZOfq7zk     11184051

--统计视频类别热度Top10
某类视频的个数作为视频类别热度
1.使用UDTF函数将类别列炸开
select
    videoId,
    category_name
from
    gulivideo_orc
lateral view explode(category) tmp_category as category_name;t1

2.按照category_name进行分组，统计每种类别视频的总数，同时按照该总数进行倒序排名，取前10
select
    category_name,
    count(*) category_count
from
    t1
group by
    category_name
order by
    category_count desc
limit 10;

最终SQL：
select
    category_name,
    count(*) category_count
from
    (select
    videoId,
    category_name
from
    gulivideo_orc
lateral view explode(category) tmp_category as category_name)t1
group by
    category_name
order by
    category_count desc
limit 10;

Music   179049
Entertainment   127674
Comedy  87818
Animation       73293
Film    73293
Sports  67329
Gadgets 59817
Games   59817
Blogs   48890
People  48890


--统计视频观看数Top20所属类别以及类别包含的Top20的视频个数
1.统计视频观看数Top20
select
    videoId,
    views,
    category
from
    gulivideo_orc
order by
    views desc
limit 20;t1

2.对t1表中的category进行炸裂
select
    videoId,
    category_name
from
    t1
lateral view explode(category) tmp_category as category_name;t2

3.对t2表进行分组(category_name)求和(总数)
select
    category_name,
    count(*) category_count
from
    t2
group by
    category_name
order by
    category_count desc;


最终SQL：
select
    category_name,
    count(*) category_count
from
    (select
    videoId,
    category_name
from
    (select
    videoId,
    views,
    category
from
    gulivideo_orc
order by
    views desc
limit 20)t1
lateral view explode(category) tmp_category as category_name)t2
group by
    category_name
order by
    category_count desc;

Entertainment   6
Comedy  6
Music   5
People  2
Blogs   2
UNA     1


--统计视频观看数Top50所关联视频的所属类别Rank
1.统计视频观看数Top50
select
    relatedId,
    views
from
    gulivideo_orc
order by
    views desc
limit 50;t1

2.对t1表中的relatedId进行炸裂并去重
select
    related_id
from
    t1
lateral view explode(relatedId) tmp_related as related_id
group by related_id;t2

3.取出观看数前50视频关联ID视频的类别
select
    category
from
    t2
join gulivideo_orc orc
on t2.related_id=orc.videoId;t3

4.对t3表中的category进行炸裂
select
    explode(category) category_name
from
    t3;t4

5.分组(类别)求和(总数)
select
    category_name,
    count(*) category_count
from
    t4
group by
    category_name
order by
    category_count desc;

最终SQL：
select
    category_name,
    count(*) category_count
from
    (select
    explode(category) category_name
from
    (select
    category
from
    (select
    related_id
from
    (select
    relatedId,
    views
from
    gulivideo_orc
order by
    views desc
limit 50)t1
lateral view explode(relatedId) tmp_related as related_id
group by related_id)t2
join gulivideo_orc orc
on t2.related_id=orc.videoId)t3)t4
group by
    category_name
order by
    category_count desc;

Comedy  232
Entertainment   216
Music   195
Blogs   51
People  51
Film    47
Animation       47
News    22
Politics        22
Games   20
Gadgets 20
Sports  19
Howto   14
DIY     14
UNA     13
Places  12
Travel  12
Animals 11
Pets    11
Autos   4
Vehicles        4


--统计上传视频最多的用户Top10以及他们上传的观看次数在前20视频
1.统计上传视频最多的用户Top10
select
    uploader,
    videos
from
    gulivideo_user_orc
order by
    videos desc
limit 10;t1

2.取出这10个人上传的所有视频,按照观看次数进行排名,取前20
select
    video.videoId,
    video.views
from
    t1
join
    gulivideo_orc video
on
    t1.uploader=video.uploader
order by
    views desc
limit 20;

最终SQL：
select
    video.videoId,
    video.views
from
    (select
    uploader,
    videos
from
    gulivideo_user_orc
order by
    videos desc
limit 10)t1
join
    gulivideo_orc video
on
    t1.uploader=video.uploader
order by
    views desc
limit 20;


--统计每个类别中的视频热度Top10
--统计每个类别中视频流量Top10
--统计每个类别视频观看数Top10
gulivideo_category
1.给每一种类别根据视频观看数添加rank值(倒序)
select
    categoryId,
    videoId,
    views,
    rank() over(partition by categoryId order by views desc) rk
from
    gulivideo_category;

2.过滤前十
select
    categoryId,
    videoId,
    views
from
    (select
    categoryId,
    videoId,
    views,
    rank() over(partition by categoryId order by views desc) rk
from
    gulivideo_category)t1
where
    rk<=10;