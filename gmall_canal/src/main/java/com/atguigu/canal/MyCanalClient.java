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

/**
 * Created by Smexy on 2021/9/4
 *
 *      ①创建一个 Canal客户端对象
 *              CanalConnector
 *                      SimpleCanalConnector： 普通模式
 *
 *                      ClusterCanalConnector: 集群版本connector实现，自带了failover功能.
 *                              可以借助zk，在两台机器安装canal，一台默认为active，另一台我standby，类似hadoop HA！
 *
 *               使用CanalConnectors创建客户端对象！
 *      ②使用客户端对象 连接上 canal server
 *      ③订阅canal server收到的binlog日志
 *      ④拉取canal server端的 日志
 *      ⑤解析日志
 *              Message: 代表拉取到的一批SQL引起的数据变化情况。
 *
 *                      List<Entry> entries：  所有SQL引起的数据变化
 *
 *              Entry： 一条SQL引起的数据变化
 *
 *                      private CanalEntry.Header header_
 *                              private Object tableName_ :  当前SQL是操作的哪张表
 *
 *                      private CanalEntry.EntryType entryType_： 当前SQL的类型
 *                              begin : 事务开启类型
 *
 *                              commit | rollback : 事务结束类型
 *
 *                              insert|update|delete : RowData类型(引起每行数据变化的类型)
 *
 *
 *                      private ByteString storeValue_ ： 存储引起的数据的变化。序列化，无法直接使用！
 *                              使用 RowChange转换为 RowChange对象
 *
 *
 *              RowChange： 转换后的storeValue
 *
 *                  EVentType： 当前SQL是什么具体SQL
 *
 *                  RowDataList : 当前sql引起的所有行的变化
 *
 *                      RowData：   一行数据的变化
 *                          Column: 一列数据的变化
 *
 *
 *
 *
 *      ⑥将解析到的日志数据写入kafka
 */
public class MyCanalClient {

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
        canalConnector.subscribe("210422test.order_info");

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
                    parseData(entry.getStoreValue());

                }
            }
        }


    }

    private static void parseData(ByteString storeValue) throws InvalidProtocolBufferException {

        //反序列化为RowChange
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

        /*
                只要 order_info表的 insert语句引起的数据变化

                order_info 不允许删数据 ，没有delete
                        只允许更新order_status

                   GMV: 统计的是所有新增订单的 total_amount
         */

        if (rowChange.getEventType() == CanalEntry.EventType.INSERT){

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

                //System.out.println(jsonObject.toJSONString());
                MyProducer.sendRecord(MyConstants.GMALL_ORDER_INFO,jsonObject.toJSONString());

            }

        }

    }
}
