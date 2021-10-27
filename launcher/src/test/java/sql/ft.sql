--SQL
--********************************************************************--
--Author: gaochen1@vipkid.com.cn
--CreateTime: 2021-06-25 10:51:52
--Comment: welcome
--自定义时间启动位点，offsetReset请设置成custom_timestamp
--********************************************************************--
--实时ft中间结果信息
create table app_inline_room_anticipation_ft_in (
    tm timestamp,
    classroom varchar,
    classId varchar,
    role varchar,
    courseId varchar,
    finalMark varchar,
    finishType varchar,
    duration varchar,
    desc varchar,
    --ts TIMESTAMP as proc_time,
    --tss TIMESTAMP,
    WATERMARK FOR tm AS withOffset(tm, 5000)
) with (
    type = 'kafka',
    topic = 'app_inline_room_anticipation_ft',
    groupId = 'app_inline_room_anticipation_ft_vklink',
    bootstrapServers = 'localhost:9092',
    offsetReset = 'custom_timestamp',
    parallelism = '1',
    timezone = 'Asia/Shanghai'
);
create table mysql_inline_room_anticipation_finishtype_by_role_sink (
    tm timestamp,
    classroom varchar,
    class_id varchar,
    course_id varchar,
    role varchar,
    user_id varchar,
    final_mark varchar,
    finish_type varchar,
    duration varchar,
    primary key(classroom, class_id, role)
) WITH (type = 'console');

insert into
    mysql_inline_room_anticipation_finishtype_by_role_sink
SELECT
    LOCALTIMESTAMP as tm,
    classroom,
    classId as class_id,
    courseId as course_id,
    role,
    '' as user_id,
    '1' as final_mark,
    max(
        if(
            finishType in ('SNS', 'TNS', 'SIT', 'TIT'),
            finishType,
            'AS'
        )
    ) as finish_type,
    SPLIT_INDEX(
        max(
            concat(
                if(
                    finishType in ('SNS', 'TNS', 'SIT', 'TIT'),
                    finishType,
                    'AS'
                ),
                '-',
                duration
            )
        ),
        '-',
        1
    ) as duration
FROM
    app_inline_room_anticipation_ft_in
GROUP BY
    TUMBLE(tm, INTERVAL '1' MINUTE),
    classroom,
    classId,
    courseId,
    role;
