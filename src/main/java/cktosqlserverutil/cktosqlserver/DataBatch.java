package cktosqlserverutil.cktosqlserver;

import java.util.List;
import java.util.Map;

public class DataBatch {
    private int batchNumber;
    private List<Map<String, Object>> data;
    private boolean poison = false; // 毒丸标记
    
    public DataBatch(int batchNumber, List<Map<String, Object>> data) {
        this.batchNumber = batchNumber;
        this.data = data;
    }
    
    // 创建毒丸批次（用于通知线程退出）
    public static DataBatch poison() {
        DataBatch batch = new DataBatch(-1, null);
        batch.poison = true;
        return batch;
    }
    
    public int getBatchNumber() { return batchNumber; }
    public List<Map<String, Object>> getData() { return data; }
    public boolean isPoison() { return poison; }
}