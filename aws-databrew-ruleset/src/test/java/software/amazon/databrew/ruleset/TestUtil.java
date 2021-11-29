package software.amazon.databrew.ruleset;

import software.amazon.awssdk.services.databrew.model.*;
import software.amazon.awssdk.services.databrew.model.ColumnSelector;
import software.amazon.awssdk.services.databrew.model.Rule;
import software.amazon.awssdk.services.databrew.model.Threshold;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtil {
    public static final String RULESET_NAME = "ruleset-name";
    public static final String RULESET_NAME_2 = "ruleset-name-2";
    public static final String RULESET_NAME_1 = "ruleset-name-1";
    public static final String RULESET_NAME_3= "ruleset-name-3";
    public static final String INVALID_RULESET_NAME = "!!";
    public static final String RULESET_DESCRIPTION = "ruleset description";
    public static final String RULESET_TARGET_ARN = "arn:aws:databrew:us-east-1:123456789012:dataset/test-ruleset-ds";
    public static final String RULESET_TARGET_ARN_1 = "arn:aws:databrew:us-east-1:123456789012:dataset/test-ruleset-ds-1";
    public static final String RULESET_TARGET_ARN_2 = "arn:aws:databrew:us-east-1:123456789012:dataset/test-ruleset-ds-2";
    public static final String RULESET_TARGET_ARN_3 = "arn:aws:databrew:us-east-1:123456789012:dataset/test-ruleset-ds-3";
    public static final String INVALID_RULESET_TARGET_ARN = "arn:aws:databrew:us-east-1:123456789012:ds/test-ruleset-ds";
    public static final String RULE_NAME = "rule-name";

    public static final Map<String, String> sampleTags() {
        Map<String, String> tagMap = new HashMap<>();
        tagMap.put("test1Key", "test1Value");
        tagMap.put("test2Key", "test12Value");
        return tagMap;
    }

    public static Rule createRuleWithoutColumnSelectors(String ruleName) {
        return Rule.builder()
                .name(ruleName)
                .checkExpression(":col1 >= :val1")
                .substitutionMap(createSubstitutionMap())
                .threshold(createThreshold())
                .build();
    }

    public static Rule createRuleWithColumnSelector(String ruleName) {
        return Rule.builder()
                .name(ruleName)
                .checkExpression("> :val1")
                .substitutionMap(createSubstitutionMap())
                .threshold(createThreshold())
                .columnSelectors(createColumnSelectorsList())
                .build();
    }

    public static Map<String, String> createSubstitutionMap() {
        Map<String, String> substitutionMap = new HashMap<>();
        substitutionMap.put("col1", "test_col");
        substitutionMap.put("val1", "5");
        return substitutionMap;
    }

    public static Threshold createThreshold() {
        return Threshold.builder()
                .type(ThresholdType.LESS_THAN)
                .unit("COUNT")
                .value(1.0)
                .build();
    }

    public static ColumnSelector createColumnSelector(String column_name) {
        return ColumnSelector.builder()
                .name(column_name)
                .build();
    }

    public static List<ColumnSelector> createColumnSelectorsList() {
        List<ColumnSelector> columnSelectorList = new ArrayList<ColumnSelector>();
        columnSelectorList.add(createColumnSelector("column_name1"));
        columnSelectorList.add(createColumnSelector("column_name2"));
        columnSelectorList.add(createColumnSelector("column_name3"));
        return columnSelectorList;
    }

    public static List<Rule> createRulesList() {
        List<Rule> ruleList = new ArrayList<Rule>();
        ruleList.add(createRuleWithoutColumnSelectors("rule_name1"));
        ruleList.add(createRuleWithColumnSelector("rule_name2"));
        return ruleList;
    }

    public static List<Rule> createInvalidRulesList() {
        List<Rule> ruleList = new ArrayList<Rule>();
        return ruleList;
    }

    public static Map<String, String> createTags() {
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("key1", "val1");
        return tags;
    }

    public static void assertThatRulesetModelsAreEqual(final Object rawModel,
                                                   final RulesetItem sdkModel) {
        assertThat(rawModel).isInstanceOf(software.amazon.databrew.ruleset.ResourceModel.class);
        software.amazon.databrew.ruleset.ResourceModel model = (software.amazon.databrew.ruleset.ResourceModel)rawModel;
        assertThat(model.getName()).isEqualTo(sdkModel.name());
    }
}
