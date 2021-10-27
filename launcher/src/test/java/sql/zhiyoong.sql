--SQL
--********************************************************************--
--Author: liutao4@vipkid.com.cn
--CreateTime: 2020-12-09 11:43:53
--Comment: welcome
--自定义时间启动位点，offsetReset请设置成custom_timestamp
--********************************************************************--
--call_map_student
create table call_student_map_in (
    student_id varchar
	,batch_id varchar
	,create_time varchar
	,update_time varchar
	,staff_id varchar
	,mobile varchar
	,__delete varchar as is_delete
	,__timestamp  varchar as is_timestamp

) with (
    type = 'kafka',
    bootstrapServers = '10.23.2.223:9092,10.23.4.130:9092,10.23.2.224:9092',
    offsetReset = 'custom_timestamp',
    topic = 'callcenter-uc_call_student_map',
    groupId = 'callcenter-uc_call_student_map_123',
    parallelism = '1',
    timezone = 'Asia/Shanghai'
);

create view call_student_map_view as
select
    batch_id
    ,last_value(student_id) as student_id
    ,last_value(create_time) as create_time
    ,last_value(update_time) as update_time
    ,last_value(staff_id) as staff_id
    ,last_value(mobile) as mobile

from
	call_student_map_in
where cast(is_delete as varchar) = '0'
group by batch_id
;

---t_call_log
create table t_calllog_in (
    id varchar as t_calllog_id
	,businessid varchar
	,starttime varchar
	,callstate varchar as call_state
	,userno varchar
	,releasereason varchar
	,ringtimelen varchar
	,talktimelen varchar
	,calldirection varchar
	,termdept varchar
  	,endtime varchar as end_time
  	,queuename varchar as queue_name
	,__delete varchar as is_delete
	,__timestamp  varchar as is_timestamp

) with (
    type = 'kafka',
    bootstrapServers = '10.23.2.223:9092,10.23.4.130:9092,10.23.2.224:9092',
    offsetReset = 'custom_timestamp',
    topic = 'callcenter-t_calllog',
    groupId = 'callcenter-t_calllog_123',
    parallelism = '1',
    timezone = 'Asia/Shanghai'
);

create view t_calllog_view as
select
    t_calllog_id
	,businessid
	,last_value(starttime) as start_time
    ,last_value(end_time) as end_time
    ,last_value(queue_name) as queue_name
	,last_value(call_state) as call_state
	,last_value(userno) as user_no
	,last_value(releasereason) as release_reason
	,last_value(ringtimelen) as ringtime_len
	,last_value(talktimelen) as talktime_len
	,last_value(calldirection) as call_direction
	,last_value(termdept) as term_dept
   from
	t_calllog_in
where cast(is_delete as varchar) = '0'
and cast(termdept as varchar) in ('cc','tmk')
and businessid is not null
and cast(businessid as varchar) not in ('null','')
group by t_calllog_id,businessid
;
--calllog
create view t_calllog_map_view as
select
	tc.t_calllog_id                 --外呼流水id
    ,tc.businessid as business_id   --业务id
	,tc.start_time                  --呼叫开始时间
    ,tc.end_time                    --呼叫结束时间
    ,tc.queue_name                  --呼叫队列
	,tc.call_state                  --呼叫状态：0：未接通，1：接通
	,tc.user_no                     --登陆用户ID
	,tc.release_reason              --挂断原因：1：忙，2：无应答，3：拒接，4：对方挂断，5：本方挂断，6：其他
	,tc.ringtime_len                --振铃时长秒
	,tc.talktime_len                --通话时长秒
	,tc.call_direction              --呼叫方向：1：呼入，2：呼出，3：转接，4：会议
	,tc.term_dept                   --终端部门名称
	,sm.student_id                  --学生id
    ,sm.batch_id                    --电话拨打唯一ID
    ,sm.staff_id                    --拨打员工ID
    ,case when cast(tc.call_direction as varchar)='2' and (cast(tc.ringtime_len as int)>=5 or cast(tc.call_state as int) = 1 or (cast(tc.ringtime_len as int)<5 and cast(tc.release_reason as int)=4)) then '1' else '0' end as if_vaild_call
from t_calllog_view tc
left join call_student_map_view sm
on tc.businessid = sm.batch_id
;

-- create table print_out_test_student_info(
--   student_id varchar
-- )WITH(
--     type ='console'
--  );

-- insert into print_out_test_student_info
-- select
-- 	student_id
-- from call_student_map_in
-- ;



create table rl_vk_dwd_evt_calllog (
    t_calllog_id     varchar             --外呼流水id
	,business_id     varchar             --业务id
	,start_time      varchar             --呼叫开始时间
	,end_time        varchar             --呼叫结束时间
	,queue_name      varchar             --呼叫队列
	,call_state      varchar             --呼叫状态：0：未接通，1：接通
	,user_no         varchar             --登陆用户ID
	,release_reason  varchar             --挂断原因：1：忙，2：无应答，3：拒接，4：对方挂断，5：本方挂断，6：其他
	,ringtime_len    varchar             --振铃时长秒
	,talktime_len    varchar             --通话时长秒
	,call_direction  varchar             --呼叫方向：1：呼入，2：呼出，3：转接，4：会议
	,term_dept       varchar             --终端部门名称
	,student_id      varchar             --学生id
	,batch_id        varchar             --电话拨打唯一ID
	,staff_id        varchar             --拨打员工ID
	,if_vaild_call   varchar
) with (
    type = 'doris',   --使用自定义结果表标示 固定
    class = 'com.vipkid.blink.doris.TableSink2Doris',  --底层调用 固定
    username = 'vk_dw',  --doris用户名
    password = 'dE5EBKlgYCHVJ4aa',    --doris密码
    db_name = 'vk_dwd', --doris库名
    table_name = 'rl_vk_dwd_evt_calllog', --doris表名
    jdbcport = '9000', --doris jdbc端口号
    httpport = '9001', --doris rest端口号
    rollcount = '20000', --条件阈值
    rollinterval = '60000', --时间阈值（单位：毫秒）
    host = '10.29.27.154', -- doris ip地址
    dingdingurl = 'https://oapi.dingtalk.com/robot/send?access_token=d732d9eed67f931a4d44845a1fdff4876e9fb2561d4b5f430020ebd6ad76dff1',
    receiversmail = 'tanbin3@vipkid.com.cn',
    ccmail = 'lilinlin@vipkid.com.cn'
);


insert into rl_vk_dwd_evt_calllog
select
    t_calllog_id
	,business_id
	,start_time
	,end_time
	,queue_name
	,call_state
	,user_no
	,release_reason
	,ringtime_len
	,talktime_len
	,call_direction
	,term_dept
	,student_id
	,batch_id
	,staff_id
	,if_vaild_call
from t_calllog_map_view;
