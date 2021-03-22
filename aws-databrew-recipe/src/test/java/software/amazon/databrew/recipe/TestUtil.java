package software.amazon.databrew.recipe;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.databrew.model.Recipe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtil {
    public static final String RECIPE_NAME = "recipe-test";
    public static final String RECIPE_DESCRIPTION = "recipe description";
    public static final String INVALID_RECIPE_NAME = "invalid-recipe-test";
    public static final Action STEP_ACTION = Action.builder()
            .operation("REMOVE_VALUES")
            .parameters(new HashMap<String,String>() {{
                put("sourceColumn", "source");
                put("pattern", "pattern");
                put("targetColumn", "target");
                put("fromTimeZone", "UTC-07:00");
                put("FromTimeZone", "UTC-07:00");
                put("toTimeZone", "Africa/Accra");
                put("ToTimeZone", "Africa/Accra");
                put("FunctionStepType", "CONVERT_TIMEZONE");
            }})
            .build();
    public static final Action STEP_ACTION_RECIPE_PARAMETERS = Action.builder()
            .operation("REMOVE_VALUES")
            .parameters(RecipeParameters.builder()
                    .sourceColumn("source")
                    .pattern("pattern")
                    .targetColumn("target")
                    .dateTimeFormat("yyyy-mm-dd")
                    .build())
            .build();
    public static final List<ConditionExpression> CONDITIONS = ImmutableList.of(software.amazon.databrew.recipe.ConditionExpression.builder()
            .condition("GREATER_THAN")
            .value("10")
            .targetColumn("target")
            .build());
    public static final List<ConditionExpression> INVALID_CONDITIONS = ImmutableList.of(software.amazon.databrew.recipe.ConditionExpression.builder()
            .condition("GREATER")
            .value("10")
            .targetColumn("target")
            .build());
    public static final List<RecipeStep> RECIPE_STEPS = ImmutableList.of(software.amazon.databrew.recipe.RecipeStep.builder()
            .action(STEP_ACTION)
            .conditionExpressions(CONDITIONS)
            .build(),
            software.amazon.databrew.recipe.RecipeStep.builder()
                    .action(STEP_ACTION_RECIPE_PARAMETERS)
                    .conditionExpressions(CONDITIONS)
                    .build());
    public static final List<RecipeStep> INVALID_RECIPE_STEPS = ImmutableList.of(software.amazon.databrew.recipe.RecipeStep.builder()
            .action(STEP_ACTION)
            .conditionExpressions(INVALID_CONDITIONS)
            .build(),
            software.amazon.databrew.recipe.RecipeStep.builder()
                    .action(STEP_ACTION_RECIPE_PARAMETERS)
                    .conditionExpressions(INVALID_CONDITIONS)
                    .build());
    public static final Map<String, String> sampleTags() {
        Map<String, String> tagMap = new HashMap<>();
        tagMap.put("test1Key", "test1Value");
        tagMap.put("test2Key", "test12Value");
        return tagMap;
    }
    public static void assertThatRecipeModelsAreEqual(final Object rawModel, final Recipe sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getName()).isEqualTo(sdkModel.name());
    }
}
