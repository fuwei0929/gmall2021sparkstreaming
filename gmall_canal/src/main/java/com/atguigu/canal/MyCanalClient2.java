package com.atguigu.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.MyConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

/**
 *
 */
public class MyCanalClient2 {

    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {

        /*
            SocketAddress address,: canal server的主机名和端口号。参考  canal.properties
                        canal.ip
                        canal.port
            String destination: 选择当前客户端订阅的数据来自哪个mysql,填写对应的instance.properties所在的目录，
                                参考canal.properties
                         canal.destinations = example,example2,example3
             String username: 1.1.4之后提供
             String password: 1.1.4之后提供
         */
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop103", 11111), "example", null, null);

        canalConnector.connect();

        // GMV: 只需要 order_info表的数据变化
        canalConnector.subscribe("210422test.*");

        //源源不断拉数据
        while(true){

            //如果当前批次没有拉取到数据，Message 的id = -1
            Message message = canalConnector.get(100);

            if (message.getId() == -1){

                System.out.println("当前没有最新数据，歇会...");
                //没有拉取到数据，歇会再去拉
                Thread.sleep(5000);

                continue;

            }

            //System.out.println(message);

            List<CanalEntry.Entry> entries = message.getEntries();

            for (CanalEntry.Entry entry : entries) {
                //获取到每一个SQL引起的数据变化

                //获取表名
                String tableName = entry.getHeader().getTableName();

                //判断类型是不是 RowData
                if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA){

                    //说明这个语句是 INSERT|UPDATE|DELETE|CREATE语句
                    parseData(tableName,entry.getStoreValue());

                }
            }
        }


    }

    private static void parseData(String tableName,ByteString storeValue) throws InvalidProtocolBufferException {

        //反序列化为RowChange
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

        /*
                只要 order_info表的 insert语句引起的数据变化

                order_info 不允许删数据 ，没有delete
                        只允许更新order_status

                   GMV: 统计的是所有新增订单的 total_amount
         */

        // order_info : insert
        if (tableName.equals("order_info") && rowChange.getEventType() == CanalEntry.EventType.INSERT){

            sendDataToKafka(MyConstants.GMALL_ORDER_INFO,rowChange);

            // order_detail : insert
        }else if (tableName.equals("order_detail") && rowChange.getEventType() == CanalEntry.EventType.INSERT){

            sendDataToKafka(MyConstants.GMALL_ORDER_DETAIL,rowChange);
            // user_info:  insert | update
        }else if(tableName.equals("user_info") && ( rowChange.getEventType() == CanalEntry.EventType.INSERT || rowChange.getEventType() == CanalEntry.EventType.UPDATE )){

            sendDataToKafka(MyConstants.GMALL_USER_INFO,rowChange);

        }

    }

    private  static  void sendDataToKafka(String topic, CanalEntry.RowChange rowChange){
        //获取当前这个insert语句引起的所有行的数据变化
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

        for (CanalEntry.RowData rowData : rowDatasList) {

            JSONObject jsonObject = new JSONObject();

                /*
                        update:   将一列的值，进行更新，会记录 更新前的值和更新后的值

                        insert:   新增的这列之后新增后的值
                 */
            //获取更新后所有列
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            for (CanalEntry.Column column : afterColumnsList) {

                jsonObject.put(column.getName(),column.getValue());

            }

            //模拟网络延迟，让Order_detail和order_info 无法同时达到kafka
            int delaySecond = new Random().nextInt(5);

           /* try {
                Thread.sleep(delaySecond * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            //System.out.println(jsonObject.toJSONString());
            MyProducer.sendRecord(topic,jsonObject.toJSONString());

        }
    }
}
