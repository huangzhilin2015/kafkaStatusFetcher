# kafkaStatusFetcher<br>
提供接口抓取kafkaServer端数据，包括集群状态、commitedOffset、HW 、LSO等等<br>
## 接口列表<br>
### 1.获取分组列表
   - 调用示例
   ```java
   Map<Node, List<GroupOverview>>  groups = ExternalEntrance.getGroups();
   ```
   - 参数描述：无
   - 返回描述：返回每个节点和对应节点的GroupOverview
### 2.查询group详情
   - 调用示例
   ```java
    String groups = "group1,group2";
    Map<String, DescribeGroupsResponse.GroupMetadata> metadatas = ExternalEntrance.describeGroups(groups);
   ```
   - 参数描述：groups为groupId列表，多个groupId以逗号分隔
   - 返回描述：对应group下的state、protocol、protocolType和group下的所有消费者的详情
### 3.获取当前存活的节点
   - 调用示例
   ```java
   List<Broker> brokers = ExternalEntrance.getSurvivalNodes();
   ```
   - 参数描述：无
   - 返回描述：返回当前存活的节点列表详情
### 4.获取某分组下TopicPartition log最大offset
   - 调用示例
   ```java
   String groupId = "group1";
   List<TopicPartition> requestInfo = new ArrayList<>();
   requestInfo.add(new TopicPartition("testTopic",0));//topic=testTopic,partition=0
   requestInfo.add(new TopicPartition("testTopic",1));//topic=testTopic,partition=1
   requestInfo.add(new TopicPartition("testTopic2",0));//topic=testTopic2,partition=1
   Map<TopicPartition, OffsetData> hw = ExternalEntrance.getHighWaterMarkOffset(groupId, requestInfo);
   ```
   - 参数描述：
        1.  groupId：要查询的groupId
        2.  requestInfo：要查询的topic partition列表，TopicPartition为org.apache.kafka.common.TopicPartition
   - 返回描述：返回每个TopicPartition和对应的OffsetData，offset为HW
### 5.获取某分组下指定TopicPartition当前消费位置
   - 调用示例   
   ```java
    String group="group1";
    List<TopicPartition> requestInfo = new ArrayList<>();
    requestInfo.add(new TopicPartition("testTopic",0));//topic=testTopic,partition=0
    requestInfo.add(new TopicPartition("testTopic",1));//topic=testTopic,partition=1
    requestInfo.add(new TopicPartition("testTopic2",0));//topic=testTopic2,partition=1
    Map<TopicPartition, OffsetAndMetadata> commited = ExternalEntrance.getCommitedOffset(groupId, requestInfo);
   ```
   - 参数描述：
        1.  groupId：要查询的groupId
        2.  requestInfo：要查询的topic partition列表，TopicPartition为org.apache.kafka.common.TopicPartition
   - 返回描述：返回每个TopicPartition和对应的OffsetAndMetadata，offset为改TopicPartition在group下的最新提交offset
## 使用说明
- 使用步骤
    1.  sbt shell->package->生成jar包->将jar包安装到本地仓库
        ```maven 
            mvn install:install-file -DgroupId=com.yichat -DartifactId=kafkastatusfetcher_2.12 -Dversion=0.0.1 -Dpackaging=jar -Dfile=E:\kafkastatusfetcher_2.12-0.0.1.jar
        ```
    2.  添加maven依赖
        ```maven
            <dependency>
                <groupId>com.yichat</groupId>
                <artifactId>kafkastatusfetcher_2.12</artifactId>
                <version>0.0.1</version>
            </dependency>
        ```
    3.  resources目录增加：KafkaMonitorConfig.properties(以下示例)
        ```properties
            #kafka集群地址,多个由逗号分隔
            bootstrap.servers=192.168.0.192:9092
            #zookeeper连接地址
            #如果kafka使用的不是根路径,需要配置对应的路径
            zookeeper.connect=192.168.0.192:281
            #NetworkClient模式
            client.provide.mode=1
            #clientPollSize
            client.poll.size=5
            zookeeper.session.timeout=3000
            zookeeper.connect.timeout=3000
            security.protocol=PLAINTEXT
            sasl.mechanism=GSSAPI
            common.request.timeout=5000
        ```
