package org.apache.drill.exec.store.druid.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DruidUtils {
    public static String andFilterAtIndex(String leftFilter,
                                            String rightFilter) throws JsonProcessingException {
        DruidAndFilter druidAndFilter = new DruidAndFilter(Arrays.asList(leftFilter, rightFilter));
        return druidAndFilter.toJson();
    }

    public static String orFilterAtIndex(String leftFilter,
                                           String rightFilter) throws JsonProcessingException {
        DruidOrFilter druidOrFilter = new DruidOrFilter(Arrays.asList(leftFilter, rightFilter));
        return druidOrFilter.toJson();
    }
}
