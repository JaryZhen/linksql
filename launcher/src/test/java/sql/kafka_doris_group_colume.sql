
create table student_online_class_in(
    role STRING,
    user_type STRING,
    local_ping STRING,
    line STRING,
    server_rtt STRING,
    classroom STRING,
    full_ping STRING,
    timestamp bigint as tm
) with (
    type = 'kafka',
    topic = 'app_inline_room_anticipation_ft_sql_test',
    groupId = 'doris_student_online_class_vklink',
    bootstrapServers = '10.32.8.10:9092,10.32.8.5:9092',
    --offsetReset = 'custom_timestamp',
    parallelism = '1',
    timezone = 'Asia/Shanghai'
);


create table student_online_class_in2(
    role varchar,
    user_type varchar,
    local_ping varchar,
    line varchar,
    server_rtt varchar,
    classroom varchar,
    full_ping varchar,
    timestamp Integer as tm
) with (
    type = 'kafka',
    topic = 'app_inline_room_anticipation_ft_sql_test',
    groupId = 'doris_student_online_class_vklink2',
    bootstrapServers = '10.32.8.10:9092,10.32.8.5:9092',
    --offsetReset = 'custom_timestamp',
    parallelism = '1',
    timezone = 'Asia/Shanghai'
);

create view sv as
select classroom,
    count(tm) as tm
from student_online_class_in
group by classroom;

create view sv2 as
select classroom,
    count(tm) as tm
from student_online_class_in2
group by classroom;

create view insert_in as
select sv.classroom as classroom, sv.tm as server_rtt, sv2.tm as full_ping, (sv.tm+sv2.tm) as tm
    from sv
    left join sv2
    on sv.classroom = sv2.classroom;

create table student_online_class_sink(
    classroom String,
    --role varchar,
   -- user_type varchar,
    ---local_ping varchar,
    --line varchar,
    server_rtt BIGINT,
    full_ping BIGINT,
    tm BIGINT
) with (
    type = 'doris',
    aggregationkey = 'classroom',
    username = 'tester_arch_rw',
    password = '8987979UJIJM.-=0',
    db_name = 'vklink',
    table_name = 'student_online_class',
    jdbcport = '9000',
    httpport = '9001',
    rollcount = '5',
    rollinterval = '1000000',
    ---alertphone = '18211058793',
    print = 'true',
    host = '172.24.101.57' -- doris ip地址
);
insert into
    student_online_class_sink
SELECT
    classroom,
    server_rtt,
    full_ping,
    tm
from
    insert_in ;
