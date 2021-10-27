create table student_online_class_in(
    role varchar,
    user_type varchar,
    local_ping varchar,
    line varchar,
    server_rtt varchar,
    classroom varchar,
    full_ping varchar,
    timestamp timestamp as tm,
    WATERMARK FOR tm AS withOffset(tm , 100000 )
) with (
    type = 'kafka',
    topic = 'app_inline_room_anticipation_ft_sql_test',
    groupId = 'doris_student_online_class_vklink',
    bootstrapServers = 'localhost:9092',
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
    timestamp timestamp as tm
) with (
    type = 'kafka',
    topic = 'app_inline_room_anticipation_ft_sql_test',
    groupId = 'doris_student_online_class_vklink',
    bootstrapServers = 'localhost:9092',
    --offsetReset = 'custom_timestamp',
    parallelism = '1',
    timezone = 'Asia/Shanghai'
);
create table student_online_class_sink(
    classroom varchar,
    role varchar,
    user_type varchar,
    local_ping varchar,
    line varchar,
    server_rtt varchar,
    full_ping varchar,
    tm timestamp
) with (
    type = 'doris',
    username = 'tester_arch_rw',
    password = '8987979UJIJM.-=0',
    aggregationkey = 'classroom',
    db_name = 'vklink',
    table_name = 'student_online_class',
    jdbcport = '9000',
    httpport = '9001',
    rollcount = '1',
    rollinterval = '2000',
    ---alertphone = '18211058793',
    parallelism = '1',
    timezone = 'Asia/Shanghai',
    print = 'true',
    host = ' ' -- doris ip地址
);
create view sink2 as
select
    *
from
    (
        select
            classroom,
            role,
            user_type,
            local_ping,
            line,
            server_rtt,
            full_ping,
            tm
        from
            student_online_class_in
        union all
        select
            classroom,
            role,
            user_type,
            local_ping,
            line,
            server_rtt,
            full_ping,
            tm
        from
            student_online_class_in2
    );
insert into
    student_online_class_sink
SELECT
    classroom,
    role,
    user_type,
    local_ping,
    line,
    server_rtt,
    full_ping,
    tm
from
    sink2;
