package io.mykidong.iceberg.example.dataapi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.lmax.disruptor.dsl.Disruptor;
import io.mykidong.iceberg.example.dataapi.component.DisruptorHttpReceiver;
import io.mykidong.iceberg.example.dataapi.domain.EventLog;
import io.mykidong.iceberg.example.dataapi.domain.EventLogTranslator;
import io.mykidong.iceberg.example.dataapi.domain.SparkStreamingInstance;
import io.mykidong.iceberg.example.dataapi.util.JsonUtils;
import io.mykidong.iceberg.example.dataapi.util.SparkStreamingContextCreator;
import io.mykidong.iceberg.example.dataapi.util.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.springframework.web.bind.annotation.RequestMethod.POST;

@RestController
public class CollectController  extends AbstractController implements InitializingBean{

    private static Logger LOG = LoggerFactory.getLogger(CollectController.class);

    @Autowired
    private Disruptor<EventLog> eventLogDisruptor;

    @Autowired
    private DisruptorHttpReceiver disruptorHttpReceiver;

    private ObjectMapper mapper = new ObjectMapper();


    public static ConcurrentMap<String, Thread> threadMap;
    private static final Object lock = new Object();

    private String hiveMetastoreUrl;
    private String bucket;
    private String accessKey;
    private String secretKey;
    private String endpoint;

    @Override
    public void afterPropertiesSet() throws Exception {
        // hive metastore.
        hiveMetastoreUrl = env.getProperty("hive.metastore.url");

        // s3 credential.
        bucket = env.getProperty("s3.credential.bucket");
        if(bucket.equals("")) {
            bucket = StringUtils.getEnv("S3_CREDENTIAL_BUCKET");
        }
        LOG.info("bucket: {}", bucket);

        accessKey = env.getProperty("s3.credential.accessKey");
        if(accessKey.equals("")) {
            accessKey = StringUtils.getEnv("S3_CREDENTIAL_ACCESS_KEY");
        }
        LOG.info("accessKey: {}", accessKey);

        secretKey = env.getProperty("s3.credential.secretKey");
        if(secretKey.equals("")) {
            secretKey = StringUtils.getEnv("S3_CREDENTIAL_SECRET_kEY");
        }
        LOG.info("secretKey: {}", secretKey);

        endpoint = env.getProperty("s3.credential.endpoint");
        if(endpoint.equals("")) {
            endpoint = StringUtils.getEnv("S3_CREDENTIAL_ENDPOINT");
        }
        LOG.info("endpoint: {}", endpoint);


        System.getenv().forEach((k, v) -> {
            LOG.info(k + ":" + v);
        });
    }

    public static Thread singletonEventLogToIceberg(DisruptorHttpReceiver receiver,
                                                    String user,
                                                    String metastoreUrl,
                                                    String bucket,
                                                    String accessKey,
                                                    String secretKey,
                                                    String endpoint) {
        if(threadMap == null) {
            synchronized(lock) {
                if(threadMap == null) {
                    threadMap = new ConcurrentHashMap<>();
                    Thread t = saveEventLogToIceberg(receiver, user, metastoreUrl, bucket, accessKey, secretKey, endpoint);
                    LOG.info("thread to save event logs to iceberg for the user [{}]", user);
                    threadMap.put(user, t);
                }
            }
        }
        else
        {
            synchronized(lock) {
                if (!threadMap.containsKey(user)) {
                    Thread t = saveEventLogToIceberg(receiver, user, metastoreUrl, bucket, accessKey, secretKey, endpoint);
                    LOG.info("thread to save event logs to iceberg for the user [{}]", user);
                    threadMap.put(user, t);
                }
            }
        }

        return threadMap.get(user);
    }

    private static Thread saveEventLogToIceberg(DisruptorHttpReceiver receiver,
                                                String user,
                                                String metastoreUrl,
                                                String bucket,
                                                String accessKey,
                                                String secretKey,
                                                String endpoint) {
        Thread t = new Thread(() -> {
            SparkStreamingInstance sparkStreamingInstance = SparkStreamingContextCreator.singleton(user, metastoreUrl, bucket, accessKey, secretKey, endpoint);
            JavaStreamingContext ssc = sparkStreamingInstance.getJavaStreamingContext();
            SparkSession spark = sparkStreamingInstance.getSpark();

            // receive event log from disruptor consumer.
            JavaDStream<EventLog> eventLogDStream = ssc.receiverStream(receiver);

            JavaPairDStream<String, String> tupleStream = eventLogDStream.mapToPair(new StreamPair());

            JavaPairDStream<String, Iterable<String>> groupedStream = tupleStream.groupByKey();
            groupedStream.foreachRDD(rdd -> {
                try {
                    if (rdd.isEmpty()) {
                        return;
                    }
                    List<String> keys = rdd.keys().collect();
                    if (keys.size() > 0) {
                        String tableName = keys.get(0);

                        JavaRDD<Iterable<String>> jsonRdd = rdd.values();

                        List<Iterable<String>> jsonIterList = jsonRdd.collect();
                        List<String> jsonList = new ArrayList<>();
                        for (Iterable<String> jsonIter : jsonIterList) {
                            jsonList.addAll(ImmutableList.copyOf(jsonIter));
                        }
                        LOG.info("json list to be written to iceberg: {}", jsonList.size());

                        Dataset<Row> df = spark.read().json(new JavaSparkContext(spark.sparkContext()).parallelize(jsonList));
                        StructType schema = spark.table(tableName).schema();

                        // write to iceberg table.
                        Dataset<Row> newDf = spark.createDataFrame(df.javaRDD(), schema);
                        newDf.writeTo(tableName).append();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            ssc.start();
            try {
                ssc.awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        });
        t.start();

        return t;
    }


    private static class StreamPair implements PairFunction<EventLog, String, String> {
        @Override
        public Tuple2<String, String> call(EventLog eventLog) throws Exception {
            String schema = eventLog.getSchema();
            String table = eventLog.getTable();
            String tableName = "hive_prod." + schema + "." + table;
            String json = eventLog.getJson();

            return new Tuple2<>(tableName, json);
        }
    }

    @PostMapping("/v1/event_log/create")
    public String create(@RequestParam Map<String, String> params) {
        return ControllerUtils.doProcess(() -> {
            String schema = params.get("schema");
            String table = params.get("table");
            String json = params.get("json");

            if(schema == null) {
                throw new NullPointerException("schema is null!");
            }
            if(table == null) {
                throw new NullPointerException("table is null!");
            }
            if(json == null) {
                throw new NullPointerException("json is null!");
            }

            // default catalog is iceberg.
            String catalog = "iceberg";

            // user.
            String user = bucket;

            // run spark streaming job.
            CollectController.singletonEventLogToIceberg(disruptorHttpReceiver,
                    user,
                    hiveMetastoreUrl,
                    bucket,
                    accessKey,
                    secretKey,
                    endpoint);

            // public event log to disruptor.
            EventLogTranslator eventLogTranslator = new EventLogTranslator();
            eventLogTranslator.setUser(user);
            eventLogTranslator.setCatalog(catalog);
            eventLogTranslator.setSchema(schema);
            eventLogTranslator.setTable(table);
            eventLogTranslator.setJson(json);

            eventLogDisruptor.publishEvent(eventLogTranslator);

            return ControllerUtils.successMessage();
        });
    }

    @RequestMapping(path = "/v1/excel/upload", method = POST, consumes = { MediaType.MULTIPART_FORM_DATA_VALUE })
    public String uploadExcel(@RequestParam Map<String, String> params, @RequestPart MultipartFile file) throws Exception{
        return ControllerUtils.doProcess(() -> {
            String schema = params.get("schema");
            String table = params.get("table");

            // default catalog is iceberg.
            String catalog = "iceberg";

            // schema name.
            String schemaName = "hive_prod." + schema;

            // table name.
            String tableName = schemaName + "." + table;

            // user.
            String user = bucket;
            // run spark streaming job.
            CollectController.singletonEventLogToIceberg(disruptorHttpReceiver,
                    user,
                    hiveMetastoreUrl,
                    bucket,
                    accessKey,
                    secretKey,
                    endpoint);

            SparkStreamingInstance sparkStreamingInstance = SparkStreamingContextCreator.singleton(user, hiveMetastoreUrl, bucket, accessKey, secretKey, endpoint);
            SparkSession spark = sparkStreamingInstance.getSpark();

            // convert excel to json list.
            List<Map<String, Object>> rowList = excelToJson(file.getInputStream(), schemaName, tableName, spark, true);

            for(Map<String, Object> row : rowList) {
                // public event log to disruptor.
                EventLogTranslator eventLogTranslator = new EventLogTranslator();
                eventLogTranslator.setUser(user);
                eventLogTranslator.setCatalog(catalog);
                eventLogTranslator.setSchema(schema);
                eventLogTranslator.setTable(table);
                eventLogTranslator.setJson(JsonUtils.toJson(row));

                eventLogDisruptor.publishEvent(eventLogTranslator);
            }


            return ControllerUtils.successMessage();
        });
    }

    public static List<Map<String, Object>> excelToJson(InputStream is, String schemaName, String tableName, SparkSession spark, boolean runCreateTable) throws IOException {
        List<Map<String, Object>> rowList = new ArrayList<>();

        // list of cell in header row.
        List<HeaderCell> headerCells = null;

        XSSFWorkbook workbook = new XSSFWorkbook(is);
        XSSFSheet sheet = workbook.getSheetAt(0);
        try {
            long count = 0;
            Iterator<org.apache.poi.ss.usermodel.Row> rowIterator = sheet.iterator();
            while (rowIterator.hasNext()) {
                org.apache.poi.ss.usermodel.Row row = rowIterator.next();
                //For each row, iterate through all the columns
                Iterator<Cell> cellIterator = row.cellIterator();
                if(count == 0) {
                    // list of cell in header row.
                    headerCells = new ArrayList<>();
                    while (cellIterator.hasNext()) {
                        Cell cell = cellIterator.next();
                        String columnName = cell.getStringCellValue();
                        columnName = columnName.replaceAll(" ", "_");
                        columnName = columnName.toLowerCase();
                        LOG.info("columnName: {}", columnName);

                        HeaderCell.ColumnType columnType = null;
                        headerCells.add(new HeaderCell(columnName, columnType));
                    }
                } else {
                    // convert row to map.
                    Map<String, Object> map = new HashMap<>();
                    int position = 0;
                    while (cellIterator.hasNext()) {
                        Cell cell = cellIterator.next();

                        HeaderCell headerCell = headerCells.get(position);
                        String columnName = headerCell.getColumnName();

                        if(cell.getCellType() == CellType.NUMERIC) {
                            headerCell.setColumnType(HeaderCell.ColumnType.DOUBLE);
                            map.put(columnName, cell.getNumericCellValue());
                        } else if(cell.getCellType() == CellType.STRING) {
                            headerCell.setColumnType(HeaderCell.ColumnType.STRING);
                            map.put(columnName, cell.getStringCellValue());
                        } else if(cell.getCellType() == CellType.BOOLEAN) {
                            headerCell.setColumnType(HeaderCell.ColumnType.STRING);
                            map.put(columnName, String.valueOf(cell.getBooleanCellValue()));
                        } else if(cell.getCellType() == CellType.ERROR) {
                            headerCell.setColumnType(HeaderCell.ColumnType.STRING);
                            map.put(columnName, String.valueOf(""));
                        } else if(cell.getCellType() == CellType._NONE) {
                            headerCell.setColumnType(HeaderCell.ColumnType.STRING);
                            map.put(columnName, String.valueOf(""));
                        } else if(cell.getCellType() == CellType.BLANK) {
                            headerCell.setColumnType(HeaderCell.ColumnType.STRING);
                            map.put(columnName, String.valueOf(""));
                        } else if(cell.getCellType() == CellType.FORMULA) {
                            headerCell.setColumnType(HeaderCell.ColumnType.STRING);
                            map.put(columnName, String.valueOf(""));
                        }

                        position++;
                    }
                    rowList.add(map);
                }

                count++;
            }
            is.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // create schema if not exist.
        String createSchemaString = createSchema(schemaName);
        LOG.info("createSchemaString: {}", createSchemaString);

        // create iceberg table if not exist.
        String createTableString = createTable(tableName, headerCells);
        LOG.info("createTableString: {}", createTableString);
        if(runCreateTable) {
            spark.sql(createSchemaString);
            LOG.info("iceberg schema [{}] created.", createSchemaString);

            spark.sql(createTableString);
            LOG.info("iceberg table [{}] created.", tableName);
        }


        return rowList;
    }

    private static class HeaderCell implements Serializable {
        private String columnName;
        private ColumnType columnType;

        public HeaderCell(String columnName, ColumnType columnType) {
            this.columnName = columnName;
            this.columnType = columnType;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public void setColumnType(ColumnType columnType) {
            this.columnType = columnType;
        }

        public String getColumnName() {
            return columnName;
        }

        public ColumnType getColumnType() {
            return columnType;
        }

        public static enum ColumnType {
            DOUBLE, STRING;
        }
    }


    private static String createTable(String tableName, List<HeaderCell> headerCells) {

        int size = headerCells.size();
        int count = 0;

        String sql = "";
        sql += "CREATE TABLE IF NOT EXISTS " + tableName + " ";
        sql += "( ";
        for(HeaderCell headerCell : headerCells) {
            String columnName = headerCell.getColumnName();
            HeaderCell.ColumnType columnType = headerCell.getColumnType();

            if(columnType == HeaderCell.ColumnType.DOUBLE) {
                sql += "  " + columnName + " double";
            } else if(columnType == HeaderCell.ColumnType.STRING) {
                sql += "  " + columnName + " string";
            }
            if(count != size -1) {
                sql += ", ";
            }
            count++;
        }
        sql += ") ";
        sql += "USING iceberg ";

        return sql;
    }

    private static String createSchema(String schema) {
        String sql = "CREATE SCHEMA IF NOT EXISTS " + schema;
        return sql;
    }
}
