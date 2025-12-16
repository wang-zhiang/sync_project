package sqlservertoes;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//category_mapping.properties 这个配置文件在resource目录下

public class Main9 {
    private static final int BULK_SIZE = 50000;
    private static final String SOURCE_DB_URL = "jdbc:sqlserver://192.168.4.219:1433;databaseName=online_dashboard_common";
    private static final String SOURCE_DB_USER = "sa";
    private static final String SOURCE_DB_PASSWORD = "smartpthdata";
    private BulkProcessor bulkProcessor;
    // 新增失败集合
    private final Set<Integer> failedCategoryIds = new ConcurrentSkipListSet<>();

    // 新增数据结构用于收集维度数据
    private static class DimensionData {
        Set<String> categories = new HashSet<>();
        Set<String> categorySegments = new HashSet<>();
        Set<String> categoryBrands = new HashSet<>();
    }

    // 数据库凭证映射配置
    private static final Map<String, DbCredentials> DB_CREDENTIALS = new HashMap<>();
    static {
        DB_CREDENTIALS.put("192.168.4.218", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.201", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.212", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.35", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.36", new DbCredentials("CHH", "Y1v606"));
        DB_CREDENTIALS.put("192.168.4.40", new DbCredentials("sa", "smartpathdata"));
        DB_CREDENTIALS.put("192.168.4.41", new DbCredentials("sa", "smartpthdata"));
        DB_CREDENTIALS.put("192.168.4.57", new DbCredentials("sa", "smartpthdata"));
    }

    private static final Map<String, String> CATEGORY3_EN_MAP = new HashMap<>();

    static {
        CATEGORY3_EN_MAP.putIfAbsent("鼻腔护理", "nasalcare");
        CATEGORY3_EN_MAP.putIfAbsent("避孕产品", "contraceptives");
        CATEGORY3_EN_MAP.putIfAbsent("冰淇淋", "icecream");
        CATEGORY3_EN_MAP.putIfAbsent("饼干", "biscuits");
        CATEGORY3_EN_MAP.putIfAbsent("早孕检测", "pregnancytest");
        CATEGORY3_EN_MAP.putIfAbsent("宠物食品", "petfood");
        CATEGORY3_EN_MAP.putIfAbsent("宠物驱虫", "petdeworming");
        CATEGORY3_EN_MAP.putIfAbsent("咖啡机", "coffeemachine");
        CATEGORY3_EN_MAP.putIfAbsent("儿科用药RX", "pediatricmedicinerx");
        CATEGORY3_EN_MAP.putIfAbsent("宝宝调味品", "babyseasoning");
        CATEGORY3_EN_MAP.putIfAbsent("宝宝番茄酱", "babyketchup");
        CATEGORY3_EN_MAP.putIfAbsent("宝宝辅食油", "babyfoodoil");
        CATEGORY3_EN_MAP.putIfAbsent("儿童食品", "childrenfood");
        CATEGORY3_EN_MAP.putIfAbsent("蜂产品", "beeproducts");
        CATEGORY3_EN_MAP.putIfAbsent("妇科用药RX", "gynecologicalmedicinerx");
        CATEGORY3_EN_MAP.putIfAbsent("肝胆用药RX", "livermedicinerx");
        CATEGORY3_EN_MAP.putIfAbsent("蛋糕", "cake");
        CATEGORY3_EN_MAP.putIfAbsent("糕点", "pastry");
        CATEGORY3_EN_MAP.putIfAbsent("锅具", "cookware");
        CATEGORY3_EN_MAP.putIfAbsent("呼吸系统RX", "respiratorymedicinerx");
        CATEGORY3_EN_MAP.putIfAbsent("彩妆", "cosmetics");
        CATEGORY3_EN_MAP.putIfAbsent("护肤品", "skincare");
        CATEGORY3_EN_MAP.putIfAbsent("身体护理", "bodycare");
        CATEGORY3_EN_MAP.putIfAbsent("香水", "perfume");
        CATEGORY3_EN_MAP.putIfAbsent("家居清洁", "householdcleaning");
        CATEGORY3_EN_MAP.putIfAbsent("织物护理", "fabriccare");
        CATEGORY3_EN_MAP.putIfAbsent("地面清洁", "floorcleaning");
        CATEGORY3_EN_MAP.putIfAbsent("地板&皮具护理", "floorleathercare");
        CATEGORY3_EN_MAP.putIfAbsent("香薰蜡烛", "scentedcandle");
        CATEGORY3_EN_MAP.putIfAbsent("坚果", "nuts");
        CATEGORY3_EN_MAP.putIfAbsent("葡萄酒", "wine");
        CATEGORY3_EN_MAP.putIfAbsent("咖啡", "coffee");
        CATEGORY3_EN_MAP.putIfAbsent("咖啡胶囊", "coffeecapsule");
        CATEGORY3_EN_MAP.putIfAbsent("抗过敏RX", "antiallergicrx");
        CATEGORY3_EN_MAP.putIfAbsent("抗菌消炎RX", "antibacterialrx");
        CATEGORY3_EN_MAP.putIfAbsent("电动牙刷", "electrictoothbrush");
        CATEGORY3_EN_MAP.putIfAbsent("美白笔&凝胶", "whiteningpengel");
        CATEGORY3_EN_MAP.putIfAbsent("漱口水", "mouthwash");
        CATEGORY3_EN_MAP.putIfAbsent("水牙线", "waterflosser");
        CATEGORY3_EN_MAP.putIfAbsent("牙粉", "toothpowder");
        CATEGORY3_EN_MAP.putIfAbsent("牙膏", "toothpaste");
        CATEGORY3_EN_MAP.putIfAbsent("牙刷", "toothbrush");
        CATEGORY3_EN_MAP.putIfAbsent("牙贴", "toothstrips");
        CATEGORY3_EN_MAP.putIfAbsent("牙线", "dentalfloss");
        CATEGORY3_EN_MAP.putIfAbsent("鸡精&味精", "chickenpowdermsg");
        CATEGORY3_EN_MAP.putIfAbsent("酵母", "yeast");
        CATEGORY3_EN_MAP.putIfAbsent("面粉", "flour");
        CATEGORY3_EN_MAP.putIfAbsent("食用油", "cookingoil");
        CATEGORY3_EN_MAP.putIfAbsent("滤水设备", "waterfilter");
        CATEGORY3_EN_MAP.putIfAbsent("猫砂", "catlitter");
        CATEGORY3_EN_MAP.putIfAbsent("脱毛仪", "hairremover");
        CATEGORY3_EN_MAP.putIfAbsent("泌尿系统RX", "urinarymedicinerx");
        CATEGORY3_EN_MAP.putIfAbsent("成人奶粉", "adultmilkpowder");
        CATEGORY3_EN_MAP.putIfAbsent("婴幼儿奶粉", "infantformula");
        CATEGORY3_EN_MAP.putIfAbsent("孕妇奶粉", "pregnantmilkpowder");
        CATEGORY3_EN_MAP.putIfAbsent("白奶", "freshmilk");
        CATEGORY3_EN_MAP.putIfAbsent("含乳饮料", "milkbeverage");
        CATEGORY3_EN_MAP.putIfAbsent("奶酪 芝士", "cheese");
        CATEGORY3_EN_MAP.putIfAbsent("酸奶 益生菌饮料", "yogurtprobiotic");
        CATEGORY3_EN_MAP.putIfAbsent("内分泌系统RX", "endocrinemedicinerx");
        CATEGORY3_EN_MAP.putIfAbsent("女性卫生用品", "femininehygiene");
        CATEGORY3_EN_MAP.putIfAbsent("皮肤用药/外伤RX", "dermatologicalmedicinerx");
        CATEGORY3_EN_MAP.putIfAbsent("啤酒", "beer");
        CATEGORY3_EN_MAP.putIfAbsent("其他私处护理RX", "otherintimatecarerx");
        CATEGORY3_EN_MAP.putIfAbsent("其他药品RX", "othermedicinesrx");
        CATEGORY3_EN_MAP.putIfAbsent("巧克力", "chocolate");
        CATEGORY3_EN_MAP.putIfAbsent("头发护理", "haircare");
        CATEGORY3_EN_MAP.putIfAbsent("灭虫&灭蟑", "insecticide");
        CATEGORY3_EN_MAP.putIfAbsent("驱蚊", "mosquitorepellent");
        CATEGORY3_EN_MAP.putIfAbsent("祛疤产品", "scarremoval");
        CATEGORY3_EN_MAP.putIfAbsent("神经系统RX", "nervoussystemrx");
        CATEGORY3_EN_MAP.putIfAbsent("其他私处护理", "otherintimatecare");
        CATEGORY3_EN_MAP.putIfAbsent("私处护理", "intimatecare");
        CATEGORY3_EN_MAP.putIfAbsent("私处护理RX", "intimatecarerx");
        CATEGORY3_EN_MAP.putIfAbsent("速冻食品", "frozenfood");
        CATEGORY3_EN_MAP.putIfAbsent("保鲜袋/膜", "freshkeepingbag");
        CATEGORY3_EN_MAP.putIfAbsent("口香糖", "chewinggum");
        CATEGORY3_EN_MAP.putIfAbsent("糖", "sugar");
        CATEGORY3_EN_MAP.putIfAbsent("防脱发", "antihairloss");
        CATEGORY3_EN_MAP.putIfAbsent("电动剃须刀", "electricshaver");
        CATEGORY3_EN_MAP.putIfAbsent("电动修眉刀", "electriceyebrowtrimmer");
        CATEGORY3_EN_MAP.putIfAbsent("手动剃须刀", "manualrazor");
        CATEGORY3_EN_MAP.putIfAbsent("玩具", "toy");
        CATEGORY3_EN_MAP.putIfAbsent("NMN", "nmn");
        CATEGORY3_EN_MAP.putIfAbsent("维生素", "vitamin");
        CATEGORY3_EN_MAP.putIfAbsent("五官用药RX", "entmedicinerx");
        CATEGORY3_EN_MAP.putIfAbsent("药用洗发水", "medicatedshampoo");
        CATEGORY3_EN_MAP.putIfAbsent("消化系统RX", "digestivemedicinerx");
        CATEGORY3_EN_MAP.putIfAbsent("心脑血管RX", "cardiorx");
        CATEGORY3_EN_MAP.putIfAbsent("肉干/肉脯/卤味零食", "jerkysnack");
        CATEGORY3_EN_MAP.putIfAbsent("血液系统RX", "hematologyrx");
        CATEGORY3_EN_MAP.putIfAbsent("洋酒", "liquor");
        CATEGORY3_EN_MAP.putIfAbsent("RX肠内营养品", "enteralnutritionrx");
        CATEGORY3_EN_MAP.putIfAbsent("肠胃用药", "gastrointestinalmedicine");
        CATEGORY3_EN_MAP.putIfAbsent("调经止痛", "menstrualrelief");
        CATEGORY3_EN_MAP.putIfAbsent("儿科用药", "pediatricmedicine");
        CATEGORY3_EN_MAP.putIfAbsent("防脱生发(OTC)", "hairgrowthotc");
        CATEGORY3_EN_MAP.putIfAbsent("妇科用药", "gynecologicalmedicine");
        CATEGORY3_EN_MAP.putIfAbsent("肝胆用药", "livermedicine");
        CATEGORY3_EN_MAP.putIfAbsent("感冒咳嗽", "coldcough");
        CATEGORY3_EN_MAP.putIfAbsent("感冒咳嗽RX", "coldcoughrx");
        CATEGORY3_EN_MAP.putIfAbsent("国际肠胃药(OTC)", "internationalgastrointestinalotc");
        CATEGORY3_EN_MAP.putIfAbsent("呼吸系统用药", "respiratorymedicine");
        CATEGORY3_EN_MAP.putIfAbsent("减肥用药(通便排毒)", "weightlossmedicine");
        CATEGORY3_EN_MAP.putIfAbsent("抗过敏", "antiallergic");
        CATEGORY3_EN_MAP.putIfAbsent("免疫调节RX", "immunomodulatorrx");
        CATEGORY3_EN_MAP.putIfAbsent("男科用药RX", "andrologyrx");
        CATEGORY3_EN_MAP.putIfAbsent("皮肤用药/外伤", "dermatologicalmedicine");
        CATEGORY3_EN_MAP.putIfAbsent("其他药品", "othermedicines");
        CATEGORY3_EN_MAP.putIfAbsent("五官用药", "entmedicine");
        CATEGORY3_EN_MAP.putIfAbsent("眼科用药", "ophthalmicmedicine");
        CATEGORY3_EN_MAP.putIfAbsent("止痛镇痛(内服)", "analgesicoral");
        CATEGORY3_EN_MAP.putIfAbsent("止痛镇痛(外用)", "analgesictopical");
        CATEGORY3_EN_MAP.putIfAbsent("痔疮用药", "hemorrhoidmedicine");
        CATEGORY3_EN_MAP.putIfAbsent("茶饮料", "teadrink");
        CATEGORY3_EN_MAP.putIfAbsent("功能饮料", "energydrink");
        CATEGORY3_EN_MAP.putIfAbsent("果蔬汁 果蔬味饮料", "juicedrink");
        CATEGORY3_EN_MAP.putIfAbsent("即饮咖啡", "rtdcoffee");
        CATEGORY3_EN_MAP.putIfAbsent("矿泉水 纯净水", "mineralwater");
        CATEGORY3_EN_MAP.putIfAbsent("碳酸饮料", "soda");
        CATEGORY3_EN_MAP.putIfAbsent("隐形眼镜&护理液", "contactlenssolution");
        CATEGORY3_EN_MAP.putIfAbsent("婴幼儿液态奶", "infantliquidmilk");
        CATEGORY3_EN_MAP.putIfAbsent("传统滋补", "traditionaltonic");
        CATEGORY3_EN_MAP.putIfAbsent("蛋白粉", "proteinpowder");
        CATEGORY3_EN_MAP.putIfAbsent("防脱生发(HF)", "hairgrowthhf");
        CATEGORY3_EN_MAP.putIfAbsent("辅酶Q10", "coenzymeq10");
        CATEGORY3_EN_MAP.putIfAbsent("关节维生素", "jointvitamin");
        CATEGORY3_EN_MAP.putIfAbsent("过敏护理", "allergycare");
        CATEGORY3_EN_MAP.putIfAbsent("减肥塑身", "weightloss");
        CATEGORY3_EN_MAP.putIfAbsent("酵素", "enzyme");
        CATEGORY3_EN_MAP.putIfAbsent("矿物质", "mineral");
        CATEGORY3_EN_MAP.putIfAbsent("卵磷脂", "lecithin");
        CATEGORY3_EN_MAP.putIfAbsent("美容&保健口服液", "beautytonic");
        CATEGORY3_EN_MAP.putIfAbsent("其他保健品", "othersupplements");
        CATEGORY3_EN_MAP.putIfAbsent("膳食纤维", "dietaryfiber");
        CATEGORY3_EN_MAP.putIfAbsent("益生菌", "probiotic");
        CATEGORY3_EN_MAP.putIfAbsent("鱼油", "fishoil");
        CATEGORY3_EN_MAP.putIfAbsent("植物精华", "plantextract");
        CATEGORY3_EN_MAP.putIfAbsent("止痛镇痛(内服)RX", "analgesicoralrx");
        CATEGORY3_EN_MAP.putIfAbsent("止痛镇痛(外用)RX", "analgesictopicalrx");
        CATEGORY3_EN_MAP.putIfAbsent("湿巾", "wetwipes");
        CATEGORY3_EN_MAP.putIfAbsent("肿瘤用药RX", "oncologyrx");
        CATEGORY3_EN_MAP.putIfAbsent("鼻腔喷剂RX", "nasalsprayrx");

        CATEGORY3_EN_MAP.putIfAbsent("地板&皮革/家具护理", "floor_leather_furniture_care");
        CATEGORY3_EN_MAP.putIfAbsent("内分泌系统RX-糖尿病", "endocrine_rx_diabetes");
        CATEGORY3_EN_MAP.putIfAbsent("心脑血管RX-其他", "cardiovascular_rx_other");
        CATEGORY3_EN_MAP.putIfAbsent("男科用药RX-壮阳", "men_health_rx_aphrodisiac");
        CATEGORY3_EN_MAP.putIfAbsent("家居清洁湿巾", "home_cleaning_wipes");
        CATEGORY3_EN_MAP.putIfAbsent("内分泌系统RX-甲状腺", "endocrine_rx_thyroid");
        CATEGORY3_EN_MAP.putIfAbsent("内分泌系统RX-减重", "endocrine_rx_weight_loss");
        CATEGORY3_EN_MAP.putIfAbsent("心脑血管RX-高血压", "cardiovascular_rx_hypertension");
        CATEGORY3_EN_MAP.putIfAbsent("心脑血管RX-高血脂高胆固醇", "cardiovascular_rx_hyperlipidemia");
        CATEGORY3_EN_MAP.putIfAbsent("男科用药RX-防脱", "men_health_rx_hair_loss");
        CATEGORY3_EN_MAP.putIfAbsent("男科用药RX-前列腺", "men_health_rx_prostate");
        CATEGORY3_EN_MAP.putIfAbsent("男科用药RX-其他", "men_health_rx_other");
    }

    private RestHighLevelClient esClient;
    private Connection sourceConn;

    public static void main(String[] args) {
        System.out.println("[MAIN] 程序启动");
        new Main9().startSync();
    }

    private static void logInfo(String message) {
        System.out.println("[INFO] " + new Date() + " - " + message);
    }

    private static void logError(String message, Throwable t) {
        System.out.println("[ERROR] " + new Date() + " - " + message);
        if (t != null) {
            t.printStackTrace();
        }
    }

    public void startSync() {
        logInfo("开始同步流程");
        try {
            initializeConnections();
            List<SpCategorySql> sqlConfigs = loadSqlConfigs();
            logInfo("共加载 " + sqlConfigs.size() + " 条SQL配置");
            processAllCategories(sqlConfigs);
        } catch (Exception e) {
            logError("同步过程中发生错误", e);
        } finally {
            closeConnections();
            // 新增：输出失败汇总
            if (!failedCategoryIds.isEmpty()) {
                logError("以下分类同步失败（共" + failedCategoryIds.size() + "个）: "
                        + failedCategoryIds.stream()
                        .sorted()
                        .map(String::valueOf)
                        .collect(Collectors.joining(", ")), null);
            } else {
                logInfo("所有分类同步成功");
            }
        }
        logInfo("同步流程结束");
    }

    private void initializeConnections() throws SQLException {
        logInfo("初始化数据库连接...");
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            logInfo("JDBC驱动加载成功");

            logInfo("尝试连接源数据库: " + SOURCE_DB_URL);
            sourceConn = DriverManager.getConnection(SOURCE_DB_URL, SOURCE_DB_USER, SOURCE_DB_PASSWORD);
            logInfo("源数据库连接成功，自动提交状态: " + sourceConn.getAutoCommit());

        } catch (ClassNotFoundException e) {
            logError("找不到SQL Server驱动", e);
            throw new SQLException("JDBC驱动加载失败", e);
        } catch (SQLException e) {
            logError("数据库连接失败", e);
            throw e;
        }

        logInfo("初始化ES客户端...");
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "smartpath"));

        esClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.6.105", 9205, "http"),
                        new HttpHost("192.168.6.106", 9206, "http")
                ).setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setDefaultCredentialsProvider(credentialsProvider))
        );
        logInfo("ES客户端初始化完成");
    }

    private List<SpCategorySql> loadSqlConfigs() throws SQLException {
        logInfo("开始加载SQL配置");
        List<SpCategorySql> configs = new ArrayList<>();

        //别忘了去sqlserver上执行一步sql,把没数的在es中没索引，sqlserver上有索引的去掉，这个后续也可以添加个功能
        String sql = "SELECT Id, SpCategoryId, ChannelType, DbHost, DbPort, DbName, SqlContent FROM CL_Common_SpCategorySql where SpCategoryId = 109;";

        try (Statement stmt = sourceConn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String sqlContent = rs.getString("SqlContent");
                if (sqlContent == null || sqlContent.trim().isEmpty()) {
                    logInfo("跳过空SQL内容的配置项，SpCategoryId: " + rs.getInt("SpCategoryId"));
                    continue;
                }

                SpCategorySql config = new SpCategorySql();
                config.setId(rs.getInt("Id"));
                config.setSpCategoryId(rs.getInt("SpCategoryId"));
                config.setChannelType(rs.getInt("ChannelType"));
                config.setDbHost(rs.getString("DbHost"));
                config.setDbPort(rs.getString("DbPort"));
                config.setDbName(rs.getString("DbName"));
                config.setSqlContent(sqlContent);
                configs.add(config);

                logInfo("加载配置: ID=" + config.getId() +
                        ", CategoryID=" + config.getSpCategoryId() +
                        ", Channel=" + config.getChannelType());
            }
        }
        return configs;
    }

    private void processAllCategories(List<SpCategorySql> configs) {
        logInfo("开始处理所有分类");
        Map<Integer, List<SpCategorySql>> grouped = configs.stream()
                .collect(Collectors.groupingBy(SpCategorySql::getSpCategoryId));

        logInfo("共发现 " + grouped.size() + " 个分类需要处理");
        grouped.forEach((categoryId, sqlConfigs) -> {
            try {
                logInfo(">>> 开始处理分类: " + categoryId);
                processCategory(categoryId, sqlConfigs);
                logInfo("<<< 完成处理分类: " + categoryId);
            } catch (Exception e) {
                failedCategoryIds.add(categoryId);
                logError("处理分类 " + categoryId + " 失败", e);
                
            }
        });
    }

    // 新增初始化方法
    private void initBulkProcessor() throws Exception {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logInfo("准备提交批量数据，数量：" + request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                //logInfo("批量提交完成，耗时：" + response.getTook());
                request.requests().clear(); // 关键！释放内存
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logError("批量提交失败", failure);
            }
        };

        this.bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) -> esClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener)
                .setBulkActions(5000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(1))
                .setConcurrentRequests(2)
                .build();
    }

    private void processCategory(int categoryId, List<SpCategorySql> sqlConfigs) throws Exception {
        SpCategory category = loadSpCategory(categoryId);

        // 新增：删除旧索引（如果存在）
        String oldIndexName = category.getEsIndexName();
        deleteOldIndexIfExists(oldIndexName);

        String indexName = generateIndexName(category);

        // 初始化BulkProcessor
        initBulkProcessor();

        Map<Integer, Set<String>> timeRanges = new ConcurrentHashMap<>();
        DimensionData dimensionData = new DimensionData();

        try {
            for (SpCategorySql config : sqlConfigs) {
                try (Connection targetConn = getTargetConnection(config)) {
                    processSqlStreaming(targetConn, config, indexName, timeRanges, dimensionData);
                }
            }

            // 强制刷新剩余数据并等待完成
            logInfo("开始强制刷新BulkProcessor...");
            bulkProcessor.flush();
            logInfo("等待BulkProcessor关闭...");
            if (!bulkProcessor.awaitClose(60, TimeUnit.SECONDS)) {
                logError("BulkProcessor关闭超时", null);
            }

            updateTimeRanges(category, timeRanges);
            updateCategoryMetadata(category, indexName);
            saveDimensionData(categoryId, dimensionData);
        } catch (InterruptedException e) {
            logError("等待批量操作关闭时被中断", e);
            Thread.currentThread().interrupt();
        } finally {
            if (bulkProcessor != null) {
                try {
                    bulkProcessor.close();
                } catch (Exception e) {
                    logError("关闭BulkProcessor时出错", e);
                }
            }
            logInfo("分类处理完成，释放资源");
        }
    }

    private Connection getTargetConnection(SpCategorySql config) throws SQLException {
        String port = (config.getDbPort() == null || config.getDbPort().trim().isEmpty())
                ? "1433" : config.getDbPort();
        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s",
                config.getDbHost(), port, config.getDbName());

        DbCredentials credentials = DB_CREDENTIALS.getOrDefault(config.getDbHost(),
                new DbCredentials(SOURCE_DB_USER, SOURCE_DB_PASSWORD));

        logInfo("连接到数据库 " + config.getDbHost() + ":" + config.getDbPort() +
                "，使用用户: " + credentials.username);

        return DriverManager.getConnection(url, credentials.username, credentials.password);
    }

    private SpCategory loadSpCategory(int categoryId) throws SQLException {
        logInfo("加载分类信息，ID: " + categoryId);
        String sql = "SELECT * FROM CL_Common_SpCategory WHERE Id = ?";
        try (PreparedStatement pstmt = sourceConn.prepareStatement(sql)) {
            pstmt.setInt(1, categoryId);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                SpCategory category = new SpCategory();
                category.setId(categoryId);
                category.setCategory1(rs.getString("Category1"));
                category.setCategory2(rs.getString("Category2"));
                category.setCategory3(rs.getString("Category3"));
                category.setDataLevel(rs.getInt("DataLevel"));
                category.setEsIndexName(rs.getString("EsIndexName")); // 新增
                return category;
            }
            throw new SQLException("未找到分类: " + categoryId);
        }
    }

    private void deleteOldIndexIfExists(String indexName) {
        if (indexName == null || indexName.trim().isEmpty()) {
            logInfo("索引名为空，无需删除");
            return;
        }

        try {
            logInfo("尝试删除旧索引: " + indexName);
            boolean exists = esClient.indices().exists(
                    new GetIndexRequest(indexName), RequestOptions.DEFAULT);

            if (exists) {
                esClient.indices().delete(
                        new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
                logInfo("成功删除索引: " + indexName);
            } else {
                logInfo("索引不存在，无需删除: " + indexName);
            }
        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                logInfo("索引不存在（无需处理）: " + indexName);
            } else {
                logError("删除索引失败: " + indexName, e);
            }
        } catch (IOException e) {
            logError("删除索引时发生IO异常", e);
        }
    }

    private String generateIndexName(SpCategory category) {
        String level = category.getDataLevel() == 1 ? "sku" : "brand";

        // 获取英文名称（如果没有映射则使用原始值）
        String category3En = CATEGORY3_EN_MAP.getOrDefault(
                category.getCategory3(),
                category.getCategory3()
        );

        String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String indexName = String.format("clcommon_%s_%d_%s_%s",
                level,
                category.getId(),
                category3En,
                timestamp
        );

        logInfo("生成索引名称: " + indexName);
        return indexName;
    }

    private static final Map<String, String> FIELD_MAPPING = new HashMap<>();
    static {
        FIELD_MAPPING.put("y", "year");
        FIELD_MAPPING.put("m", "ym");
        FIELD_MAPPING.put("brandEncn", "brandEnCh");
        FIELD_MAPPING.put("subChannel", "subchannel");
        FIELD_MAPPING.put("val", "value");
        FIELD_MAPPING.put("val2", "value2");
        FIELD_MAPPING.put("val3", "value3");
        FIELD_MAPPING.put("val4", "value4");
        FIELD_MAPPING.put("val5", "value5");

    }


    // 定义需要替换 null 的字段集合（统一小写）
    private static final Set<String> NULL_REPLACEMENT_FIELDS = new HashSet<>(
            Arrays.asList("value", "value1", "value2", "value3", "value4", "value5", "volume","volume1", "volume2", "volume3", "volume4", "volume5")
    );

    private void processSqlStreaming(Connection conn, SpCategorySql config,
                                     String indexName,
                                     Map<Integer, Set<String>> timeRanges,
                                     DimensionData dimensionData) throws SQLException {
        final int channelType = config.getChannelType();
        final String sql = config.getSqlContent();
        final int batchSize = 5000; // 每批处理量




        // 使用流式结果集
        try (PreparedStatement stmt = conn.prepareStatement(
                sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {

            stmt.setFetchSize(batchSize); // 关键！流式读取

            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();

                // 预计算列索引（优化性能）
                int categoryIndex = -1, segmentIndex = -1, brandIndex = -1, ymIndex = -1;
                for (int i = 1; i <= colCount; i++) {
                    String colName = meta.getColumnName(i).toLowerCase();
                    switch (colName) {
                        case "category": categoryIndex = i; break;
                        case "segment": segmentIndex = i; break;
                        case "brandench": brandIndex = i; break;
                        case "ym": ymIndex = i; break;
                    }
                }

                int rowCount = 0;
                while (rs.next()) {
                    Map<String, Object> doc = new HashMap<>(colCount);
                    String category = "", segment = "", brand = "", ymValue = null;

                    // 处理列数据
                    for (int i = 1; i <= colCount; i++) {
                        String colName = meta.getColumnName(i);
                        Object value = rs.getObject(i);

                        // 检查字段名是否在预定义集合中
                        if (value == null && NULL_REPLACEMENT_FIELDS.contains(colName)) {
                            value = 0; // 替换为 0
                        }


                        String esFieldName = FIELD_MAPPING.getOrDefault(colName, colName); // 使用映射关系
                        doc.put(esFieldName, value);


                        if (i == categoryIndex) category = String.valueOf(value);
                        else if (i == segmentIndex) segment = String.valueOf(value);
                        else if (i == brandIndex) brand = String.valueOf(value);
                        else if (i == ymIndex) ymValue = String.valueOf(value);
                    }

                    // 处理时间范围
                    if (ymValue != null) {
                        timeRanges.computeIfAbsent(channelType, k -> new HashSet<>())
                                .add(ymValue);
                    }

                    // 处理维度数据
                    if (categoryIndex != -1 && !category.isEmpty()) {
                        dimensionData.categories.add(category);
                        if (segmentIndex != -1 && !segment.isEmpty()) {
                            dimensionData.categorySegments.add(category + "||" + segment);
                        }
                        if (brandIndex != -1 && !brand.isEmpty()) {
                            dimensionData.categoryBrands.add(category + "||" + brand);
                        }
                    }

                    addDerivedFields(doc);

                    // 实时提交到ES
                    bulkProcessor.add(new IndexRequest(indexName).source(doc));


                }
                logInfo("处理完成，共 " + rowCount + " 行数据");
            }
        }
    }
    private void addDerivedFields(Map<String, Object> doc) {
        String category = String.valueOf(doc.getOrDefault("category", ""));
        String segment = String.valueOf(doc.getOrDefault("segment", ""));
        String channel = String.valueOf(doc.getOrDefault("channel", ""));
        String subchannel = String.valueOf(doc.getOrDefault("subchannel", ""));

        doc.put("parm_category", category + "__1");
        doc.put("parm_segment", category + "__" + segment);
        doc.put("parm_channelandsubchannel", channel + "__" + subchannel+"__");
    }

    private void bulkIndexToEs(String indexName, List<Map<String, Object>> data) {
        logInfo("开始向ES索引 " + indexName + " 写入 " + data.size() + " 条数据");

        BulkProcessor bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) ->
                        esClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        logInfo("准备批量写入 " + request.numberOfActions() + " 条数据");
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        if (response.hasFailures()) {
                            logError("批量写入失败: " + response.buildFailureMessage(), null);
                        } else {
                            logInfo("成功写入 " + request.numberOfActions() + " 条数据");
                        }
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logError("批量写入发生异常", failure);
                    }
                })
                .setBulkActions(BULK_SIZE)
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .build();

        try {
            for (Map<String, Object> doc : data) {
                bulkProcessor.add(new IndexRequest(indexName).source(doc));
            }
        } finally {
            try {
                bulkProcessor.awaitClose(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logError("等待批量写入关闭时被中断", e);
            }
            logInfo("ES索引 " + indexName + " 写入流程结束");
        }
    }

    private void updateTimeRanges(SpCategory category, Map<Integer, Set<String>> timeRanges) {
        // Traditional (1+2)
        Set<String> traditionalMonths = new HashSet<>();
        traditionalMonths.addAll(timeRanges.getOrDefault(1, Collections.emptySet()));
        traditionalMonths.addAll(timeRanges.getOrDefault(2, Collections.emptySet()));
        List<String> sortedTraditionalMonths = traditionalMonths.stream()
                .sorted()
                .collect(Collectors.toList());

        logInfo("传统渠道时间记录数: " + sortedTraditionalMonths.size());
        if (!sortedTraditionalMonths.isEmpty()) {
            setTimeRange(category, sortedTraditionalMonths, "EtlTimeRangeTraditional");
            logInfo("设置传统渠道时间范围: " + category.getEtlTimeRangeTraditional());
        } else {
            logInfo("没有传统渠道时间数据");
        }

        // PDD (3)
        Set<String> pddMonths = timeRanges.getOrDefault(3, Collections.emptySet());
        List<String> sortedPddMonths = pddMonths.stream()
                .sorted()
                .collect(Collectors.toList());

        logInfo("PDD渠道时间记录数: " + sortedPddMonths.size());
        if (!sortedPddMonths.isEmpty()) {
            setTimeRange(category, sortedPddMonths, "EtlTimeRangePDD");
            logInfo("设置PDD时间范围: " + category.getEtlTimeRangePDD());
        } else {
            logInfo("没有PDD渠道时间数据");
        }

        // TikTok (4)
        Set<String> tikTokMonths = timeRanges.getOrDefault(4, Collections.emptySet());
        List<String> sortedTikTokMonths = tikTokMonths.stream()
                .sorted()
                .collect(Collectors.toList());

        logInfo("TikTok渠道时间记录数: " + sortedTikTokMonths.size());
        if (!sortedTikTokMonths.isEmpty()) {
            setTimeRange(category, sortedTikTokMonths, "EtlTimeRangeTikTok");
            logInfo("设置TikTok时间范围: " + category.getEtlTimeRangeTikTok());
        } else {
            logInfo("没有TikTok渠道时间数据");
        }
    }

    private void setTimeRange(SpCategory category, List<String> months, String field) {
        if (months == null || months.isEmpty()) {
            logInfo("设置时间范围 - 空数据，跳过");
            return;
        }

        // 增加排序验证
        List<String> sortedMonths = months.stream()
                .sorted()
                .collect(Collectors.toList());

        String min = sortedMonths.get(0);
        String max = sortedMonths.get(sortedMonths.size()-1);
        String range = min + "-" + max;

        // 增加长度校验
        if (range.length() > 50) {
            logError("时间范围超长("+range.length()+"): " + range, null);
            range = range.substring(0, 47) + "...";
        }

        logInfo("最终时间范围 ("+field+"): " + range);

        switch (field) {
            case "EtlTimeRangeTraditional":
                category.setEtlTimeRangeTraditional(range);
                break;
            case "EtlTimeRangePDD":
                category.setEtlTimeRangePDD(range);
                break;
            case "EtlTimeRangeTikTok":
                category.setEtlTimeRangeTikTok(range);
                break;
        }
    }

    private void updateCategoryMetadata(SpCategory category, String indexName) {
        String sql = "UPDATE CL_Common_SpCategory SET " +
                "EtlTimeRangeTraditional = ?, " +
                "EtlTimeRangePDD = ?, " +
                "EtlTimeRangeTikTok = ?, " +
                "LastEtlTime = GETDATE(), " +
                "EsIndexName = ? " +
                "WHERE Id = ?";

        try (PreparedStatement pstmt = sourceConn.prepareStatement(sql)) {
            // 处理空值的更安全方式
            String traditional = Optional.ofNullable(category.getEtlTimeRangeTraditional())
                    .filter(s -> !s.isEmpty()).orElse(null);
            String pdd = Optional.ofNullable(category.getEtlTimeRangePDD())
                    .filter(s -> !s.isEmpty()).orElse(null);
            String tikTok = Optional.ofNullable(category.getEtlTimeRangeTikTok())
                    .filter(s -> !s.isEmpty()).orElse(null);

            logInfo("准备更新数据库参数:");
            logInfo("Traditional: " + traditional);
            logInfo("PDD: " + pdd);
            logInfo("TikTok: " + tikTok);
            logInfo("IndexName: " + indexName);
            logInfo("CategoryID: " + category.getId());

            pstmt.setString(1, traditional);
            pstmt.setString(2, pdd);
            pstmt.setString(3, tikTok);
            pstmt.setString(4, indexName);
            pstmt.setInt(5, category.getId());

            int affectedRows = pstmt.executeUpdate();
            if (affectedRows > 0) {
                logInfo("成功更新 " + affectedRows + " 行数据");
                if (!sourceConn.getAutoCommit()) {
                    sourceConn.commit();
                    logInfo("手动提交事务");
                }
            } else {
                logError("没有更新任何数据，可能CategoryID不存在: " + category.getId(), null);
            }
        } catch (SQLException e) {
            logError("更新分类元数据失败", e);
            logError("SQL状态码: " + e.getSQLState(), null);
            logError("错误代码: " + e.getErrorCode(), null);
            logError("错误信息: " + e.getMessage(), null);
        }
    }

    //插入三个表的维度数据

    // 新增维度数据保存方法
    private void saveDimensionData(int spCategoryId, DimensionData data) {
        try {
            sourceConn.setAutoCommit(false); // 开启事务

            // 删除旧数据
            deleteOldDimensionData(spCategoryId);

            // 插入新数据
            insertCategoryData(spCategoryId, data.categories);
            insertSegmentData(spCategoryId, data.categorySegments);
            insertBrandData(spCategoryId, data.categoryBrands);

            sourceConn.commit();
            logInfo("维度数据保存成功："
                    + "\n  - 分类数: " + data.categories.size()
                    + "\n  - 分类+分段数: " + data.categorySegments.size()
                    + "\n  - 分类+品牌数: " + data.categoryBrands.size());
        } catch (SQLException e) {
            try {
                sourceConn.rollback();
                logError("维度数据保存事务回滚", e);
            } catch (SQLException ex) {
                logError("回滚失败", ex);
            }
        } finally {
            try {
                sourceConn.setAutoCommit(true);
            } catch (SQLException e) {
                logError("恢复自动提交失败", e);
            }
        }
    }

    // 删除旧数据
    private void deleteOldDimensionData(int spCategoryId) throws SQLException {
        String[] deleteSQLs = {
                "DELETE FROM CL_Common_SpCategoryCategory WHERE SpCategoryId = ?",
                "DELETE FROM CL_Common_SpCategorySegment WHERE SpCategoryId = ?",
                "DELETE FROM CL_Common_SpCategoryBrand WHERE SpCategoryId = ?"
        };

        for (String sql : deleteSQLs) {
            try (PreparedStatement pstmt = sourceConn.prepareStatement(sql)) {
                pstmt.setInt(1, spCategoryId);
                int count = pstmt.executeUpdate();
                logInfo("删除旧数据[" + sql + "]，影响行数: " + count);
            }
        }
    }

    // 插入分类数据
    private void insertCategoryData(int spCategoryId, Set<String> categories) throws SQLException {
        if (categories.isEmpty()) {
            logInfo("无分类数据需要插入");
            return;
        }

        String sql = "INSERT INTO CL_Common_SpCategoryCategory (SpCategoryId, Category) VALUES (?, ?)";
        try (PreparedStatement pstmt = sourceConn.prepareStatement(sql)) {
            int batchSize = 0;
            for (String category : categories) {
                pstmt.setInt(1, spCategoryId);
                pstmt.setString(2, category);
                pstmt.addBatch();

                if (++batchSize % 1000 == 0) {
                    pstmt.executeBatch();
                    logInfo("已提交批量分类数据: " + batchSize);
                }
            }
            int[] counts = pstmt.executeBatch();
            logInfo("插入分类数据完成，总行数: " + Arrays.stream(counts).sum());
        }
    }

    // 插入分类+分段数据
    private void insertSegmentData(int spCategoryId, Set<String> categorySegments) throws SQLException {
        if (categorySegments.isEmpty()) {
            logInfo("无分类+分段数据需要插入");
            return;
        }

        String sql = "INSERT INTO CL_Common_SpCategorySegment (SpCategoryId, Category, Segment) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = sourceConn.prepareStatement(sql)) {
            int batchSize = 0;
            for (String pair : categorySegments) {
                String[] parts = pair.split("\\|\\|", 2);
                pstmt.setInt(1, spCategoryId);
                pstmt.setString(2, parts[0]);
                pstmt.setString(3, parts.length > 1 ? parts[1] : "");
                pstmt.addBatch();

                if (++batchSize % 1000 == 0) {
                    pstmt.executeBatch();
                    logInfo("已提交批量分段数据: " + batchSize);
                }
            }
            int[] counts = pstmt.executeBatch();
            logInfo("插入分类+分段数据完成，总行数: " + Arrays.stream(counts).sum());
        }
    }

    // 插入分类+品牌数据
    private void insertBrandData(int spCategoryId, Set<String> categoryBrands) throws SQLException {
        if (categoryBrands.isEmpty()) {
            logInfo("无分类+品牌数据需要插入");
            return;
        }

        String sql = "INSERT INTO CL_Common_SpCategoryBrand (SpCategoryId, Category, Brand) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = sourceConn.prepareStatement(sql)) {
            int batchSize = 0;
            for (String pair : categoryBrands) {
                String[] parts = pair.split("\\|\\|", 2);
                pstmt.setInt(1, spCategoryId);
                pstmt.setString(2, parts[0]);
                pstmt.setString(3, parts.length > 1 ? parts[1] : "");
                pstmt.addBatch();

                if (++batchSize % 1000 == 0) {
                    pstmt.executeBatch();
                    logInfo("已提交批量品牌数据: " + batchSize);
                }
            }
            int[] counts = pstmt.executeBatch();
            logInfo("插入分类+品牌数据完成，总行数: " + Arrays.stream(counts).sum());
        }
    }

    private void closeConnections() {
        try {
            if (sourceConn != null && !sourceConn.isClosed()) {
                sourceConn.close();
                logInfo("已关闭源数据库连接");
            }
        } catch (SQLException e) {
            logError("关闭源数据库连接时出错", e);
        }

        try {
            if (esClient != null) {
                esClient.close();
                logInfo("已关闭ES客户端");
            }
        } catch (Exception e) {
            logError("关闭ES客户端时出错", e);
        }
    }

    // 数据模型类
    private static class SpCategorySql {
        private int id;
        private int spCategoryId;
        private int channelType;
        private String dbHost;
        private String dbPort;
        private String dbName;
        private String sqlContent;

        // getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public int getSpCategoryId() { return spCategoryId; }
        public void setSpCategoryId(int spCategoryId) { this.spCategoryId = spCategoryId; }
        public int getChannelType() { return channelType; }
        public void setChannelType(int channelType) { this.channelType = channelType; }
        public String getDbHost() { return dbHost; }
        public void setDbHost(String dbHost) { this.dbHost = dbHost; }
        public String getDbPort() { return dbPort; }
        public void setDbPort(String dbPort) { this.dbPort = dbPort; }
        public String getDbName() { return dbName; }
        public void setDbName(String dbName) { this.dbName = dbName; }
        public String getSqlContent() { return sqlContent; }
        public void setSqlContent(String sqlContent) { this.sqlContent = sqlContent; }
    }

    private static class SpCategory {
        private int id;
        private String category1;
        private String category2;
        private String category3;
        private int dataLevel;
        private String etlTimeRangeTraditional;
        private String etlTimeRangePDD;
        private String etlTimeRangeTikTok;
        private String esIndexName;

        // getters and setters
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getCategory1() { return category1; }
        public void setCategory1(String category1) { this.category1 = category1; }
        public String getCategory2() { return category2; }
        public void setCategory2(String category2) { this.category2 = category2; }
        public String getCategory3() { return category3; }
        public void setCategory3(String category3) { this.category3 = category3; }
        public int getDataLevel() { return dataLevel; }
        public void setDataLevel(int dataLevel) { this.dataLevel = dataLevel; }
        public String getEtlTimeRangeTraditional() { return etlTimeRangeTraditional; }
        public void setEtlTimeRangeTraditional(String etlTimeRangeTraditional) {
            this.etlTimeRangeTraditional = etlTimeRangeTraditional;
        }
        public String getEtlTimeRangePDD() { return etlTimeRangePDD; }
        public void setEtlTimeRangePDD(String etlTimeRangePDD) {
            this.etlTimeRangePDD = etlTimeRangePDD;
        }
        public String getEtlTimeRangeTikTok() { return etlTimeRangeTikTok; }
        public void setEtlTimeRangeTikTok(String etlTimeRangeTikTok) {
            this.etlTimeRangeTikTok = etlTimeRangeTikTok;
        }

        // Getter & Setter
        public String getEsIndexName() { return esIndexName; }
        public void setEsIndexName(String esIndexName) { this.esIndexName = esIndexName; }

    }

    private static class DbCredentials {
        private String username;
        private String password;

        public DbCredentials(String username, String password) {
            this.username = username;
            this.password = password;
        }
    }
}