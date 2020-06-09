##### bd-hermes_sink-fm: Hermes数据写入ODPS

- 编译： mvn clean install -Dmaven.test.skip=true

- 上线运行：nohup sh flume-ng agent -c /opt/apache-flume-1.9.0-bin/conf -f /opt/apache-flume-1.9.0-bin/conf/kafka_odps_sink.conf -n a1 -Dflume.monitoring.type=http -Dflume.monitoring.port=34548 -Dflume_log_file=kafka2odps >/dev/null 2>&1 &

- 更新记录
    - 1.0.0-RELEASE / 20191219
    ```java
    基础功能上线
    ```
   - 2.0.0-RELEASE / 20191219
      ```
      实时读取 kafka 写入 odps
      ```
   - 3.0.0-RELEASE / 20200526
       ```
         迁移回 s21_user_track
       ```
     
- 注意
    ```java
    kill -TERM pid 一定要用这个，不然会丢失offset问题 
    ```
