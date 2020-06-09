package com.hupu.hermes.sink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * mvn  clean  package  -Dmaven.test.skip=true
 * 运行命令
 * <p>
 * nohup  java  -jar  -Dspring.profiles.active=test  -Xmx3000M   bd-hermes_sink-fm-0.0.1-SNAPSHOT.jar  >/dev/null 2>&1 &
 * <p>
 * nohup  java  -jar  -Dspring.profiles.active=prd  --server.port=13579  -Xmx3000M   bd-hermes_sink-fm-0.0.1-SNAPSHOT.jar  >/dev/null 2>&1 &
 * <p>
 * 向生产机器传输 jar  包
 * curl -u hupu:hupu@NiuB -T  /Users/gaohui/Desktop/bd-hermes_sink-fm-boot-k-consumner/target/bd-hermes_sink-fm-3.0.0-RELEASE.jar   file.pub.hupu.com/zxl/
 * <p>
 * <p>
 * jvm 参数 :
 */


@SpringBootApplication
public class KafkaToOdpsApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaToOdpsApplication.class, args);
    }

}
