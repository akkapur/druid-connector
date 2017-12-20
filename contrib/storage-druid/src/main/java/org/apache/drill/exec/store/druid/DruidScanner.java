package org.apache.drill.exec.store.druid;

import com.google.common.collect.ImmutableList;

import java.util.List;


public class DruidScanner {

    final DruidQueryClient client;
    final String dataSource;
    int batchSizeBytes;
    long limit;
    List<String> projectedColumnNames;
    long scanRequestTimeout;

    public DruidScanner(DruidQueryClient client, String dataSource) {

        this.client = client;
        this.dataSource = dataSource;
        this.batchSizeBytes = 1048576;
        this.limit = 9223372036854775807L;
        this.projectedColumnNames = null;
    }

    public DruidScanner setProjectedColumnNames(List<String> columnNames) {
        if(columnNames != null) {
            this.projectedColumnNames = ImmutableList.copyOf(columnNames);
        } else {
            this.projectedColumnNames = null;
        }

        return this;
    }

    public DruidScanner batchSizeBytes(int batchSizeBytes) {
        this.batchSizeBytes = batchSizeBytes;
        return this;
    }

    public DruidScanner limit(long limit) {
        this.limit = limit;
        return this;
    }

    public void GetDruidScannerIterator() {

    }
}
