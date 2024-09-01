package com.riskmanager;


public class CountSql {

    // public static void main(String[] args) throws Exception {
    //     EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    //     TableEnvironment tEnv = TableEnvironment.create(settings);

    //     tEnv.executeSql("CREATE TABLE clicks (\n" +
    //             "    u_id  BIGINT FROM 'uId', \n" +
    //             "    page      VARCHAR,\n" +
    //             "    ts TIMESTAMP(3) FROM 'timestamp',\n" +
    //             "    WATERMARK FOR timestamp AS timestamp - INTERVAL '5' SECOND\n" +
    //             ") WITH (\n" +
    //             "    'connector' = 'kafka',\n" +
    //             "    'topic'     = 'input',\n" +
    //             "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
    //             "    'scan.startup.mode' = 'earliest-offset',\n" +
    //             "    'format'    = 'json'\n" +
    //             ")");

    //     tEnv.executeSql("SELECT *\n" +
    //             "FROM clicks\n" +
    //             "    MATCH_RECOGNIZE (\n" +
    //             "        PARTITION BY uid\n" +
    //             "        ORDER BY ts\n" +
    //             "        MEASURES\n" +
    //             "            A.ts AS index_tstamp,\n" +
    //             "            B.ts AS shop_tstamp\n" +
    //             "        ONE ROW PER MATCH\n" +
    //             "        PATTERN (A B)\n" +
    //             "        DEFINE\n" +
    //             "            A AS\n" +
    //             "                A.page = '/index'\n" +
    //             "            B AS\n" +
    //             "                B.page = '/shop'\n" +
    //             "    ) MR");


    // }


}
