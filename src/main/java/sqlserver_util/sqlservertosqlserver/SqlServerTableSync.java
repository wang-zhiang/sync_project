package sqlserver_util.sqlservertosqlserver;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class SqlServerTableSync {

    public void sync(SyncConfig cfg) throws Exception {
        System.out.println("启动同步: " + cfg);

        List<SqlServerSchemaUtils.ColumnInfo> cols;
        try (Connection src = DbUtil.newSource(cfg)) {
            cols = SqlServerSchemaUtils.getSourceColumns(src, cfg.sourceSchema, cfg.sourceTable);
        }

        if (cfg.mode == SyncConfig.Mode.OVERWRITE) {
            try (Connection tgt = DbUtil.newTarget(cfg);
                 Statement st = tgt.createStatement()) {
                String ddl = SqlServerSchemaUtils.buildCreateTableDDL(cols, cfg.targetSchema, cfg.targetTable);
                st.execute(ddl);
                tgt.commit();
            }
        } else {
            try (Connection tgt = DbUtil.newTarget(cfg)) {
                if (!SqlServerSchemaUtils.tableExists(tgt, cfg.targetSchema, cfg.targetTable)) {
                    try (Statement st = tgt.createStatement()) {
                        String ddl = SqlServerSchemaUtils.buildCreateTableDDL(cols, cfg.targetSchema, cfg.targetTable);
                        st.execute(ddl);
                        tgt.commit();
                    }
                }
            }
        }

        long totalRows;
        try (Connection src = DbUtil.newSource(cfg)) {
            totalRows = SqlServerSchemaUtils.countRows(src, cfg.sourceSchema, cfg.sourceTable);
        }
        System.out.println("源表总行数: " + totalRows);

        if (totalRows == 0) {
            System.out.println("源表为空，结束。");
            return;
        }

        int pageSize = cfg.pageSize;
        long pages = (totalRows + pageSize - 1) / pageSize;
        ExecutorService pool = Executors.newFixedThreadPool(cfg.threads);
        List<Future<?>> futures = new ArrayList<>();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < pages; i++) {
            final int pageIndex = i;
            final long start = (long) pageIndex * pageSize + 1;
            final long end = Math.min(start + pageSize - 1, totalRows);

            futures.add(pool.submit(() -> {
                try {
                    copyPage(cfg, cols, start, end, pageIndex);
                } catch (Exception e) {
                    throw new RuntimeException("分页 " + pageIndex + " 失败: " + e.getMessage(), e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        long ms = System.currentTimeMillis() - startTime;
        System.out.println("同步完成，耗时: " + ms + " ms");
    }

    private void copyPage(SyncConfig cfg, List<SqlServerSchemaUtils.ColumnInfo> cols,
                          long startRow, long endRow, int pageIndex) throws Exception {
        String selectSql = SqlServerSchemaUtils.buildPagedSelectSql(cfg.sourceSchema, cfg.sourceTable, cols);
        String insertSql = SqlServerSchemaUtils.buildInsertSql(cfg.targetSchema, cfg.targetTable, cols);

        try (Connection src = DbUtil.newSource(cfg);
             Connection tgt = DbUtil.newTarget(cfg);
             PreparedStatement sps = src.prepareStatement(selectSql,
                     ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             PreparedStatement ips = tgt.prepareStatement(insertSql)) {

            if (cfg.queryTimeoutSeconds > 0) {
                sps.setQueryTimeout(cfg.queryTimeoutSeconds);
                ips.setQueryTimeout(cfg.queryTimeoutSeconds);
            }
            sps.setFetchSize(cfg.fetchSize);

            sps.setLong(1, startRow);
            sps.setLong(2, endRow);

            int colCount = cols.size();
            int batchCount = 0;
            long copied = 0;

            try (ResultSet rs = sps.executeQuery()) {
                while (rs.next()) {
                    for (int i = 1; i <= colCount; i++) {
                        Object val = rs.getObject(i);
                        ips.setObject(i, val);
                    }
                    ips.addBatch();
                    batchCount++;
                    copied++;

                    if (batchCount >= cfg.batchSize) {
                        ips.executeBatch();
                        tgt.commit();
                        batchCount = 0;
                    }
                }
            }

            if (batchCount > 0) {
                ips.executeBatch();
                tgt.commit();
            }

            System.out.println("分页[" + pageIndex + "] 完成: 行 " + startRow + "-" + endRow + ", 拷贝 " + copied);
        }
    }
}