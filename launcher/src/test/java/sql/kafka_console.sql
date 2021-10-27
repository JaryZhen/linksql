create table student_online_class_in(
    role varchar,
    user_type varchar,
    local_ping varchar,
    line varchar,
    server_rtt varchar,
    classroom varchar,
    full_ping varchar,
    `timestamp` varchar as tm
) with (
    type = 'kafka',
    topic = 'app_inline_room_anticipation_ft_sql_test',
    groupId = 'doris_student_online_class_vklink',
    bootstrapServers = 'localhost:9092',
    ---offsetReset = 'custom_timestamp',
    parallelism = '1',
    timezone = 'Asia/Shanghai'
);
create table student_online_class_sink(
    role varchar,
    user_type varchar,
    local_ping varchar,
    line varchar,
    server_rtt varchar,
    classroom varchar,
    full_ping varchar,
    tm varchar
) with (type = 'console');
insert into
    student_online_class_sink
SELECT
    role,
    user_type,
    local_ping,
    line,
    server_rtt,
    classroom,
    full_ping,
    tm
from
    student_online_class_in
