package com.wwn.kclient.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.combinator.testing.Str;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: WeinanWu
 * Date: 2021-03-05
 * Time: 14:55
 */
public class KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    protected static int MULTI_MSG_ONCE_SEND_NUM = 20;

    private Producer<String, String> producer;  //KAFKA消息生产者

    private String defaultTopic;    //默认topic

    private String propertiesFile;  //属性文件
    private Properties properties;  //属性对象

    public KafkaProducer() {
        // For Spring context
    }

    public KafkaProducer(String propertiesFile, String defaultTopic) {
        this.propertiesFile = propertiesFile;
        this.defaultTopic = defaultTopic;

        init();
    }

    public KafkaProducer(Properties properties, String defaultTopic) {
        this.properties = properties;
        this.defaultTopic = defaultTopic;

        init();
    }

    protected void init() {
        //加载属性文件，初始化属性对象
        if (properties == null) {
            properties = new Properties();
            try {
                properties.load(Thread.currentThread().getContextClassLoader()
                                        .getResourceAsStream(propertiesFile));
            } catch (IOException e) {
                log.error("The properties file is not loaded.", e);
                throw new IllegalArgumentException(
                        "The properties file is not loaded.", e);
            }
        }
        log.info("Producer properties:" + properties);

        //初始化Kafka的生产者对象
        ProducerConfig config = new ProducerConfig(properties);
        producer = new Producer<String, String>(config);
    }

    //消息发送方法
    //String类型统一发送方法,发送到Topic
    public void send(String message) {
        send2Topic(null, message);
    }

    public void send2Topic(String topicName, String message) {
        if (message == null) {
            return;
        }

        if (topicName == null) {
            topicName = defaultTopic;
        }

        KeyedMessage<String ,String> km = new KeyedMessage<String, String>(
                topicName, message
        );
        producer.send(km);
    }

    //指定Key发送
    public void send(String key, String message) {
        send2Topic(null, key, message);
    }

    public void send2Topic(String topicName, String key, String message) {
        if (message == null) {
            return;
        }

        if (topicName == null) {
            topicName = defaultTopic;
        }

        KeyedMessage<String ,String> km = new KeyedMessage<String, String>(
                topicName, key, message
        );
        producer.send(km);
    }

    //发送一组集合里的String消息
    public void send(Collection<String> messages) {
        send2Topic(null, messages);
    }

    public void send2Topic(String topicName, Collection<String> messages) {

    }

    //发送Map中的消息
    public void send(Map<String, String> messages) {

    }

    public void send2Topic(String topicName, Map<String, String> messages) {

    }

    //发送bean消息
    public <T> void sendBean(T bean) {
        sendBean2Topic(null, bean);
    }

    public <T> void sendBean2Topic(String topicName, T bean) {

    }

    public <T> void sendBean(String key, T bean) {
        sendBean2Topic(null, key, bean);
    }

    public <T> void sendBean2Topic(String topicName, String key, T bean) {

    }

    public <T> void sendBeans(Collection<T> beans) {
        sendBeans2Topic(null, beans);
    }

    public <T> void sendBeans2Topic(String topicName, Collection<T> beans) {

    }

    public <T> void sendBeans(Map<String, T> beans) {
        sendBeans2Topic(null, beans);
    }

    public <T> void sendBeans2Topic(String topicName, Map<String, T> beans) {

    }

    //发送 JSON Object 消息

    public void sendObject(JSONObject jsonObject) {
        sendObject2Topic(null, jsonObject);
    }

    public void sendObject2Topic(String topicName, JSONObject jsonObject) {
        send2Topic(topicName, jsonObject.toJSONString());
    }

    public void sendObject(String key, JSONObject jsonObject) {
        sendObject2Topic(null, key, jsonObject);
    }

    public void sendObject2Topic(String topicName, String key, JSONObject jsonObject) {
        send2Topic(topicName, key, jsonObject.toJSONString());
    }

    public void sendObjects(JSONArray jsonArray) {
        sendObjects2Topic(null, jsonArray);
    }

    public void sendObjects2Topic(String topicName, JSONArray jsonArray) {
        send2Topic(topicName, jsonArray.toJSONString());
    }

    public void sendObjects(Map<String, JSONObject> jsonObjects) {
        sendObjects2Topic(null, jsonObjects);
    }

    public void sendObjects2Topic(String topicName, Map<String, JSONObject> jsonObjects) {

    }

    public void close() {
        producer.close();
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public void setPropertiesFile(String propertiesFile) {
        this.propertiesFile = propertiesFile;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
