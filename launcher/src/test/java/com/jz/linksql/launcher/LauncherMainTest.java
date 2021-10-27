package com.jz.linksql.launcher;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * @Author: Jary
 * @Date: 2021/10/27 2:34 下午
 */
public class LauncherMainTest extends TestCase {

    @Test
    public void testDoris() throws Exception {

        /*
        -mode yarnPer
-sql /data0/vkflinkplatformvklinkFile/dev_dataplat/demo/kafka_consol.sql/20210401104738/kafka_consol.sql
-name kafka_consol
-localSqlPluginPath /data/vklink/MyVKLINTest/sqlplugins
-flinkconf /data/flink-1.10.1/conf
-yarnconf /etc/hadoop/conf
-queue dev_dataplat
-pluginLoadMode shipfile
-flinkJarPath /data/flink-1.10.1/lib
-confProp {"timezone":"Asia/Shanghai","sql.checkpoint.interval":"60000"}
         *
         */
        String rootFile = "/Users/jary/IdeaProjects/solink/";
        String confProp= "{\"sql.checkpoint.cleanup.mode\":\"false\"," +
                "\"sql.checkpoint.interval\":10000," +
                "\"time.characteristic\":\"EventTime\"," + //ProcessingTime EventTime
                "\"auto.offset.reset\": \"earliest\"}";
        String sqlFilePath="launcher/src/test/java/nsql/";

        String sqlFile= sqlFilePath+"kafka_print.sql";

        String[] sql = new String[]{
                "-mode", "local",//local yarnPer getPlane
                "-sql", rootFile + sqlFile,
                "-name", "jary",
                "-localSqlPluginPath", rootFile + "sqlplugins",
                "-remoteSqlPluginPath", rootFile + "sqlplugins",
                "-flinkconf", rootFile + "conf/flinkconf",
                "-addjar", "[\""+rootFile + "sqlplugins/core-master.jar\"]",
                "-confProp", confProp,
                "-yarnconf", rootFile + "conf/yarnconf",
                "-flinkJarPath", rootFile + "conf/lib",
                "-queue", "root.dev_dataplat",
                "-pluginLoadMode", "shipfile"};
        System.setProperty("HADOOP_USER_NAME", "zhenguoyou");
        LauncherMain.main(sql);

    }

    @Test
    public void testDD() {
        String a ="";
        String[] aa  = a.split(",");
        System.out.println(aa[1]);
    }

}
