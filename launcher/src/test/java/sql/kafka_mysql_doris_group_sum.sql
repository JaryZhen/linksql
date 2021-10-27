create table student_online_class_in(
    role varchar,
    user_type varchar,
    local_ping varchar,
    line varchar,
    server_rtt varchar,
    classroom varchar,
    full_ping varchar,
    timestamp integer as tm
) with (
    type = 'kafka',
    topic = 'app_inline_room_anticipation_ft_sql_test',
    groupId = 'doris_student_online_class_vklink',
    bootstrapServers = 'localhost:9092',
    --offsetReset = 'custom_timestamp',
    parallelism = '1',
    timezone = 'Asia/Shanghai'
);
create table sideTable(
    classroom varchar,
    role varchar,
    user_type varchar,
    local_ping varchar,
    line varchar,
    server_rtt varchar,
    full_ping varchar,
    tm varchar,
    primary key (classroom),
    PERIOD FOR SYSTEM_TIME
) WITH(
    type = 'mysql',
    url = 'jdbc:mysql://localhost:9000/arch?charset=utf8',
    userName = 'tester_arch_rw',
    password = '8987979UJIJM.-=0',
    tableName = 'student_online_class_dm',
    cache = 'LRU',
    cacheSize = '100',
    cacheTTLMs = '60000',
    cacheMode = 'unordered',
    asyncCapacity = '1000',
    asyncTimeout = '10000',
    parallelism = '1',
    partitionedJoin = 'false'
);
create table student_online_class_sink(classroom varchar, tm integer) with (
    type = 'doristest',
    aggregationkey = 'classroom',
    username = 'tester_arch_rw',
    password = '8987979UJIJM.-=0',
    db_name = 'vklink',
    table_name = 'student_online_class',
    jdbcport = '9000',
    httpport = '9001',
    rollcount = '1',
    rollinterval = '1',
    alertphone = '18211058793',
    host = ' ' -- doris ip地址
);
insert into
    student_online_class_sink
SELECT
    classroom,
    tm
from
    (
        SELECT
            classroom,
            sum(tms) as tm
        from
            (
                select
                    i.classroom as classroom,
                    i.role as role,
                    s.classroom as user_type,
                    i.local_ping as local_ping,
                    i.line as line,
                    i.server_rtt as server_rtt,
                    i.full_ping as full_ping,
                    i.tm as tms
                from
                    student_online_class_in i
                    join sideTable s on i.classroom = s.classroom
            )
        group by classroom
    )
where tm < 5
