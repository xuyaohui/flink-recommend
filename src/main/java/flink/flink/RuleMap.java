package flink.flink;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RuleMap {

    private RuleMap(){}

    public final static Map<String,List<AlertRule>> initialRuleMap;

    private static List<AlertRule> ruleList = new ArrayList<>();

    private static List<String> ruleStringList = new ArrayList<>(Arrays.asList(
            "{\"target\":\"MathVal\",\"type\":\"0\",\"criticalVal\":90,\"descInfo\":\"You Math score is too low\"}",
            "{\"target\":\"MathVal\",\"type\":\"2\",\"criticalVal\":140,\"descInfo\":\"You Math score is too high\"}",
            "{\"target\":\"PhysicsVal\",\"type\":\"0\",\"criticalVal\":60,\"descInfo\":\"You Physics score is too low\"}",
            "{\"target\":\"PhysicsVal\",\"type\":\"2\",\"criticalVal\":95,\"descInfo\":\"You Physics score is too high\"}"));

    static {
        for (String i : ruleStringList) {
            ruleList.add(JSON.parseObject(i, AlertRule.class));
        }
        initialRuleMap = ruleList.stream().collect(Collectors.groupingBy(AlertRule::getTarget));
    }

}
