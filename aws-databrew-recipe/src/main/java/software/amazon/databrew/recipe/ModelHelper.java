package software.amazon.databrew.recipe;

import software.amazon.awssdk.services.databrew.model.ConditionExpression;
import software.amazon.awssdk.services.databrew.model.DescribeRecipeResponse;
import software.amazon.awssdk.services.databrew.model.Recipe;
import software.amazon.awssdk.services.databrew.model.RecipeAction;
import software.amazon.awssdk.services.databrew.model.RecipeStep;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ModelHelper {
    public static ResourceModel constructModel(final DescribeRecipeResponse recipe) {
        Map<String, String> tags = recipe.tags();
        return ResourceModel.builder()
                .description(recipe.description())
                .name(recipe.name())
                .steps(buildModelRecipeSteps(recipe.steps()))
                .tags(tags != null ? buildModelTags(tags) : null)
                .build();
    }

    public static ResourceModel constructModel(final Recipe recipe) {
        Map<String, String> tags = recipe.tags();
        return ResourceModel.builder()
                .description(recipe.description())
                .name(recipe.name())
                .steps(buildModelRecipeSteps(recipe.steps()))
                .tags(tags != null ? buildModelTags(tags) : null)
                .build();
    }

    public static Map<String, String> buildTagInputMap(final List<Tag> tagList) {
        Map<String, String> tagMap = new HashMap<>();
        // return null if no Tag specified.
        if (tagList == null) return null;

        for (Tag tag : tagList) {
            tagMap.put(tag.getKey(), tag.getValue());
        }
        return tagMap;
    }

    public static List<Tag> buildModelTags(final Map<String, String> tags) {
        List<Tag> tagArrayList = new ArrayList<>();
        if (tags == null) return null;
        tags.forEach((k, v) -> tagArrayList.add(Tag.builder().key(k).value(v).build()));
        return tagArrayList;
    }

    public static List<software.amazon.databrew.recipe.RecipeStep> buildModelRecipeSteps(final List<RecipeStep> requestRecipeSteps) {
        List<software.amazon.databrew.recipe.RecipeStep> modelRecipeSteps = new ArrayList<>();
        if (requestRecipeSteps != null) {
            requestRecipeSteps.forEach(recipeStep -> {
                ObjectMapper mapper = new ObjectMapper();
                Map<String, String> tempMap = recipeStep.action().parameters();
                Map<String, String> parametersMap = new HashMap<>();
                if (tempMap != null) {
                    tempMap.forEach((key, value) -> parametersMap.put(Character.toUpperCase(key.charAt(0)) + key.substring(1), value));
                }
                RecipeParameters recipeParameters = mapper.convertValue(parametersMap, RecipeParameters.class);
                Action modelStepAction = Action.builder()
                        .operation(recipeStep.action().operation())
                        .parameters(recipeParameters)
                        .build();

                List<software.amazon.databrew.recipe.ConditionExpression> modelConditions = new ArrayList<>();
                List<ConditionExpression> resultConditions = recipeStep.conditionExpressions();
                if(resultConditions != null){
                    resultConditions.forEach(condition ->{
                        software.amazon.databrew.recipe.ConditionExpression stepCondition = software.amazon.databrew.recipe.ConditionExpression.builder()
                                .condition(condition.condition())
                                .value(condition.value())
                                .targetColumn(condition.targetColumn())
                                .build();
                        modelConditions.add(stepCondition);
                    });
                }

                software.amazon.databrew.recipe.RecipeStep modelRecipeStep = software.amazon.databrew.recipe.RecipeStep.builder()
                        .action(modelStepAction)
                        .conditionExpressions(modelConditions)
                        .build();
                modelRecipeSteps.add(modelRecipeStep);
            });
        }
        return modelRecipeSteps;
    }

    public static List<RecipeStep> buildRequestRecipeSteps(final List<software.amazon.databrew.recipe.RecipeStep> modelRecipeSteps) {
        List<RecipeStep> requestRecipeSteps = new ArrayList<>();
        if (modelRecipeSteps != null) {
            modelRecipeSteps.forEach(step -> {
                Action modelRecipeAction = step.getAction();
                RecipeParameters parameters = modelRecipeAction.getParameters();
                ObjectMapper m = new ObjectMapper();
                Map<String, String> tempMap = m.convertValue(parameters, new TypeReference<Map<String, String>>() {
                });
                Map<String, String> parametersMap = new HashMap<>();
                if (tempMap != null) {
                    tempMap.forEach((key, value) -> parametersMap.put(Character.toLowerCase(key.charAt(0)) + key.substring(1), value));
                }
                RecipeAction requestRecipeAction = RecipeAction.builder()
                        .operation(modelRecipeAction.getOperation())
                        .parameters(parametersMap)
                        .build();

                List<software.amazon.databrew.recipe.ConditionExpression> modelConditionExpressions = step.getConditionExpressions();
                List<ConditionExpression> requestConditionExpressions = new ArrayList<>();
                if (modelConditionExpressions != null){
                    modelConditionExpressions.forEach(condition -> {
                        ConditionExpression requestConditionExp = ConditionExpression.builder()
                                .condition(condition.getCondition())
                                .value(condition.getValue())
                                .targetColumn(condition.getTargetColumn())
                                .build();
                        requestConditionExpressions.add(requestConditionExp);
                    });
                }

                RecipeStep recipeStep = RecipeStep.builder()
                        .action(requestRecipeAction)
                        .conditionExpressions(requestConditionExpressions)
                        .build();
                requestRecipeSteps.add(recipeStep);
            });
        }
        return requestRecipeSteps;
    }
}
