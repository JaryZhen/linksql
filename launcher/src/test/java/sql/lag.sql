--SQL
--********************************************************************--
--Author: gaochen1@vipkid.com.cn
--CreateTime: 2021-06-29 14:37:28
--Comment: welcome
--自定义时间启动位点，offsetReset请设置成custom_timestamp
--********************************************************************--
create table dim__vk_dwa_pty_user_test_info_da_in (
    role varchar,
    user_id varchar,
    user_name varchar,
    certificate_flag varchar,
    if_dm varchar
) with (
    type = 'kafka',
    bootstrapServers = '10.23.2.223:9092,10.23.4.130:9092,10.23.2.224:9092',
    offsetReset = 'custom_timestamp',
    topic = 'vk_dwa_pty_user_test_info_da',
    groupId = 'vk_dwa_pty_user_test_info_da_group_test_alert',
    kafka.fetch.wait.max.ms = '100000000',
    kafka.fetch.message.max.bytes = '1'

);
CREATE TABLE dim__vk_dwa_pty_user_test_info_da(
    role varchar,
    user_id varchar,
    user_name varchar,
    certificate_flag varchar,
    if_dm varchar,
    primary key (user_id)
) WITH(
    type = 'console'
);
insert into
    dim__vk_dwa_pty_user_test_info_da
SELECT
    role,
    user_id,
    user_name,
    certificate_flag,
    if_dm
from
    dim__vk_dwa_pty_user_test_info_da_in;
