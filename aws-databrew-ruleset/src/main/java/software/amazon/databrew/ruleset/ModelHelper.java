package software.amazon.databrew.ruleset;

import software.amazon.awssdk.services.databrew.model.DescribeRulesetResponse;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class ModelHelper {

    public static software.amazon.databrew.ruleset.ResourceModel constructModel(final DescribeRulesetResponse ruleset) {
        Map<String, String> tags = ruleset.tags();
        software.amazon.databrew.ruleset.ResourceModel model = software.amazon.databrew.ruleset.ResourceModel.builder()
                .name(ruleset.name())
                .description(ruleset.description())
                .targetArn(ruleset.targetArn())
                .rules(buildModelRules(ruleset.rules()))
                .tags(tags != null ? buildModelTags(tags) : null)
                .build();
        return model;
    }

    public static software.amazon.databrew.ruleset.ResourceModel constructModel(final software.amazon.awssdk.services.databrew.model.RulesetItem rulesetItem) {
        Map<String, String> tags = rulesetItem.tags();
        software.amazon.databrew.ruleset.ResourceModel model = software.amazon.databrew.ruleset.ResourceModel.builder()
                .name(rulesetItem.name())
                .description(rulesetItem.description())
                .targetArn(rulesetItem.targetArn())
                .tags(tags != null ? buildModelTags(tags) : null)
                .build();
        return model;
    }

    public static List<software.amazon.awssdk.services.databrew.model.ColumnSelector> buildRequestColumnSelectors(final List<software.amazon.databrew.ruleset.ColumnSelector> modelColumnSelectors) {
        List<software.amazon.awssdk.services.databrew.model.ColumnSelector> requestColumnSelectors = new ArrayList<>();
        if (modelColumnSelectors == null) {
            return null;
        }
        modelColumnSelectors.forEach(modelColumnSelector -> {
            software.amazon.awssdk.services.databrew.model.ColumnSelector requestProfileColumnSelector = software.amazon.awssdk.services.databrew.model.ColumnSelector.builder()
                    .name(modelColumnSelector.getName())
                    .regex(modelColumnSelector.getRegex())
                    .build();
            requestColumnSelectors.add(requestProfileColumnSelector);
        });
        return requestColumnSelectors;
    }

    public static List<software.amazon.databrew.ruleset.ColumnSelector> buildModelColumnSelectors(final List<software.amazon.awssdk.services.databrew.model.ColumnSelector> requestColumnSelectors) {
        List<software.amazon.databrew.ruleset.ColumnSelector> modelProfileColumnSelectors = new ArrayList<>();
        if (requestColumnSelectors == null) {
            return null;
        }
        requestColumnSelectors.forEach(requestColumnSelector -> {
            software.amazon.databrew.ruleset.ColumnSelector modelColumnSelector = ColumnSelector.builder()
                    .name(requestColumnSelector.name())
                    .regex(requestColumnSelector.regex())
                    .build();
            modelProfileColumnSelectors.add(modelColumnSelector);
        });
        return modelProfileColumnSelectors;
    }

    public static software.amazon.awssdk.services.databrew.model.Threshold buildRequestThreshold(final Threshold modelThreshold) {
        return modelThreshold == null ? null : software.amazon.awssdk.services.databrew.model.Threshold.builder()
                .value(modelThreshold.getValue())
                .type(modelThreshold.getType())
                .unit(modelThreshold.getUnit())
                .build();
    }

    public static Threshold buildModelThreshold(final software.amazon.awssdk.services.databrew.model.Threshold requestThreshold) {
        return requestThreshold == null ? null : Threshold.builder()
                .value(requestThreshold.value())
                .type(requestThreshold.type().toString())
                .unit(requestThreshold.unit().toString())
                .build();
    }

    private static List<SubstitutionValue> buildModelSubstitutionMap(Map<String, String> requestSubstitutionMap) {
        if (requestSubstitutionMap == null) return null;
        List<software.amazon.databrew.ruleset.SubstitutionValue> substitutionMapList = new ArrayList<>();
        requestSubstitutionMap.forEach((k, v) -> substitutionMapList.add(software.amazon.databrew.ruleset.SubstitutionValue
                .builder().valueReference(k).value(v).build()));
        return substitutionMapList;
    }

    private static Map<String, String> buildRequestSubstitutionMap(List<SubstitutionValue> modelMapList) {
        if (modelMapList == null) return null;
        Map<String, String> requestSubstitutionMap = new HashMap<String, String>();
        for (SubstitutionValue fv: modelMapList) {
            requestSubstitutionMap.put(fv.getValueReference(), fv.getValue());
        }
        return requestSubstitutionMap;
    }

    public static List<software.amazon.awssdk.services.databrew.model.Rule> buildRequestRules(final List<software.amazon.databrew.ruleset.Rule> modelRules) {
        if (modelRules == null) return null;
        final List<software.amazon.awssdk.services.databrew.model.Rule> requestRules = new ArrayList<>();
        modelRules.forEach(rule -> {
            software.amazon.awssdk.services.databrew.model.Rule requestRule = software.amazon.awssdk.services.databrew.model.Rule.builder()
                    .name(rule.getName())
                    .disabled(rule.getDisabled())
                    .checkExpression(rule.getCheckExpression())
                    .substitutionMap(buildRequestSubstitutionMap(rule.getSubstitutionMap()))
                    .threshold(buildRequestThreshold(rule.getThreshold()))
                    .columnSelectors(buildRequestColumnSelectors(rule.getColumnSelectors()))
                    .build();
            requestRules.add(requestRule);
        });
        return requestRules;
    }

    public static  List<software.amazon.databrew.ruleset.Rule> buildModelRules(final List<software.amazon.awssdk.services.databrew.model.Rule> requestRules) {
        if (requestRules == null) return null;
        final List<software.amazon.databrew.ruleset.Rule> modelRules = new ArrayList<>();
        requestRules.forEach(rule -> {
            software.amazon.databrew.ruleset.Rule  modelRule = software.amazon.databrew.ruleset.Rule.builder()
                    .name(rule.name())
                    .disabled(rule.disabled())
                    .checkExpression(rule.checkExpression())
                    .substitutionMap(buildModelSubstitutionMap(rule.substitutionMap()))
                    .threshold(buildModelThreshold(rule.threshold()))
                    .columnSelectors(buildModelColumnSelectors(rule.columnSelectors()))
                    .build();
            modelRules.add(modelRule);
        });
        return modelRules;
    }

    public static Map<String, String> buildTagInputMap(final List<software.amazon.databrew.ruleset.Tag> tagList) {
        Map<String, String> tagMap = new HashMap<>();
        // return null if no Tag specified.
        if (tagList == null) return null;

        for (software.amazon.databrew.ruleset.Tag tag : tagList) {
            tagMap.put(tag.getKey(), tag.getValue());
        }
        return tagMap;
    }

    public static List<software.amazon.databrew.ruleset.Tag> buildModelTags(final Map<String, String> tags) {
        List<software.amazon.databrew.ruleset.Tag> tagArrayList = new ArrayList<>();
        if (tags == null) return null;
        tags.forEach((k, v) -> tagArrayList.add(software.amazon.databrew.ruleset.Tag.builder().key(k).value(v).build()));
        return tagArrayList;
    }

}
