package elm_mt_filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class get_itemname_where {  // 类名应该是 Main33，以符合 Java 的命名惯例

    public static List<O2OKeywords> keywords;

    public static void main(String[] args) {
        keywords = elm_mt_filter.getKeywords();
        List<Map<String, String>> finalList = new ArrayList<>();
        finalList.add(categorizeAndGenerateQuery(keywords));
        printFinalList(finalList);

    }



    public static Map<String, String> categorizeAndGenerateQuery(List<O2OKeywords> keywords) {
        Map<String, List<O2OKeywords>> categorized = new HashMap<>();
        for (O2OKeywords keyword : keywords) {
            categorized.computeIfAbsent(keyword.getCategory(), k -> new ArrayList<>()).add(keyword);
        }

        Map<String, String> queries = new HashMap<>();
        for (Map.Entry<String, List<O2OKeywords>> entry : categorized.entrySet()) {
            StringBuilder query = new StringBuilder();
            for (O2OKeywords k : entry.getValue()) {
                if (!k.getKeyword2().isEmpty()) {
                    if (query.length() > 0) {
                        query.append(" OR ");
                    }
                    query.append("(");
                    query.append("itemname LIKE '%").append(k.getKeyword1()).append("%' AND itemname LIKE '%").append(k.getKeyword2()).append("%'");
                    query.append(")");
                } else {
                    if (query.length() > 0) {
                        query.append(" OR ");
                    }
                    query.append("itemname LIKE '%").append(k.getKeyword1()).append("%'");
                }
            }
            queries.put(entry.getKey(), query.toString());
        }
        return queries;
    }

    public static void printFinalList(List<Map<String, String>> finalList) {
        for (Map<String, String> map : finalList) {
            map.forEach((category, query) -> {
                System.out.println("Category: " + category + "\nQuery: " + query);
            });
        }
    }



}