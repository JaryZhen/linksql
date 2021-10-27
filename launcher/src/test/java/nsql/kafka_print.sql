CREATE TABLE kafkaSource (
    `timestamp-type` STRING METADATA VIRTUAL,
    `timestamp` TIMESTAMP(3) METADATA,
    `leader-epoch` INT METADATA VIRTUAL,
    `headers` MAP<STRING, BYTES> METADATA,
    `partition` INT METADATA VIRTUAL,
    `topic` STRING METADATA VIRTUAL,
    `offset` BIGINT METADATA VIRTUAL,
    role STRING,  user_type STRING,
    local_ping INT,  line INT,  server_rtt INT,
    classroom STRING,  full_ping STRING,
    `ts` timestamp(3)
) WITH (
        'connector' = 'kafka',
        'topic' = 'app_inline_room_anticipation_ft_sql_test',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
        );

CREATE TABLE kafkaSink (
    role STRING,
    user_type STRING,
    local_ping INT,
    line INT,
    server_rtt INT,
    classroom STRING,
    full_ping STRING,
    `ts` timestamp(3)
) WITH (
    'connector' = 'print'
    );
insert into kafkaSink
    select role,
       user_type,
       local_ping,
       line,
       server_rtt,
       classroom,
       full_ping,
       ts
    from kafkaSource
