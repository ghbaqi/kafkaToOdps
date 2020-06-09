package com.hupu.hermes.sink.odps;

import com.aliyun.odps.utils.StringUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 配置
 */
@Component
@Slf4j
@Data
public class Configuration {


//    private static Configuration configuration;

    private Configuration() {
        init();
        partitionFileds.add("ds");
        partitionFileds.add("act");
    }

//    public synchronized static Configuration getInstance() {
//
//        if (configuration == null) {
//            configuration = new Configuration();
//
//        }
//        return configuration;
//
//    }


    private String accessId;
    private String accessKey;
    private String odpsUrl;

    private String projectName;


    private String tableName;


    private int processBatchSize = 1000;

    /**
     * session 存活时间
     * 单位 minute
     */
    private int sessionLiveTime = 10;


    /**
     * event 中的数据包含的 分区 字段是哪些
     */
    private List<String> partitionFileds = new ArrayList<>();

    /**
     * odps.project.name=sas_dev
     * odps.access.id=aid
     * odps.access.key=key
     * odps.url=
     * odps.table.name=sas_dev
     * odps.process.batch.size=6000
     */
//    @PostConstruct
    private void init() {

        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
//            String activeProfile = applicationContext.getEnvironment().getActiveProfiles()[0];
//            log.info("activeProfile = " + activeProfile);
//            if ("prd".equalsIgnoreCase(activeProfile)) {
//                inputStream = Configuration.class.getClassLoader().getResourceAsStream("odps-prd.properties");
//            } else if ("test".equalsIgnoreCase(activeProfile)) {
//                inputStream = Configuration.class.getClassLoader().getResourceAsStream("odps-test.properties");
//            } else if ("dev".equalsIgnoreCase(activeProfile)) {
//                inputStream = Configuration.class.getClassLoader().getResourceAsStream("odps-dev.properties");
//            } else {
//                throw new RuntimeException("未知的启动参数 activeProfile =  " + activeProfile);
//            }
            inputStream = Configuration.class.getClassLoader().getResourceAsStream("odps.properties");
            properties.load(inputStream);
            log.info("加载配置文件成功  project name = " + properties.getProperty("odps.project.name"));

        } catch (Exception e) {
            log.info("inputStream = {} , properties = {}", inputStream, properties);
            throw new RuntimeException("加载 odps 配置文件出错 e = " + e);
        }
        if (StringUtils.isBlank(properties.getProperty("odps.project.name"))) {
            throw new RuntimeException("必须传入 project name");
        } else {
            this.projectName = properties.getProperty("odps.project.name");
        }

        if (StringUtils.isBlank(properties.getProperty("odps.access.id"))) {
            throw new RuntimeException("必须传入 access id");
        } else {
            this.accessId = properties.getProperty("odps.access.id");
        }

        if (StringUtils.isBlank(properties.getProperty("odps.access.key"))) {
            throw new RuntimeException("必须传入 access key");
        } else {
            this.accessKey = properties.getProperty("odps.access.key");
        }

        if (!StringUtils.isBlank(properties.getProperty("odps.url"))) {
            this.odpsUrl = properties.getProperty("odps.url");
        }

        if (StringUtils.isBlank(properties.getProperty("odps.table.name"))) {
            throw new RuntimeException("必须传入 table name");
        } else {
            this.tableName = properties.getProperty("odps.table.name");
        }

        if (!StringUtils.isBlank(properties.getProperty("odps.process.batch.size"))) {
            this.processBatchSize = Integer.valueOf(properties.getProperty("odps.process.batch.size"));
        }

        if (!StringUtils.isBlank(properties.getProperty("odps.session.live.time"))) {
            this.sessionLiveTime = Integer.valueOf(properties.getProperty("odps.session.live.time"));
        }


//        if (!StringUtils.isBlank(properties.getProperty("odps.partition.fields"))) {
//            String[] arr = properties.getProperty("odps.partition.fields").split(",");
//            for (String s : arr) {
//                partitionFileds.add(s);
//            }
//
//        }


        try {
            if (inputStream != null)
                inputStream.close();
        } catch (IOException e) {
        }

    }

}
