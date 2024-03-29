
create table student_online_class_in(
    role varchar,
    user_type varchar,
    local_ping varchar,
    line varchar,
    server_rtt varchar,
    classroom varchar,
    full_ping varchar,
    timestamp varchar as tm
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
    tm varchar
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
    host = '172.24.101.57' -- doris ip地址
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
    student_online_class_in;
