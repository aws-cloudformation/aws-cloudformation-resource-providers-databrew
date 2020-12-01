package software.amazon.databrew.project;

import software.amazon.awssdk.services.databrew.model.*;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtil {

    public static final String PROJECT_NAME = "project-test";
    public static final String INVALID_PROJECT_NAME = "project-test-invalid";
    public static final String VALID_ARN_ROLE = "arn:aws:iam::1234567890:role/PassRoleAdmin";
    public static final Map<String, String> sampleTags() {
        Map<String, String> tagMap = new HashMap<>();
        tagMap.put("test1Key", "test1Value");
        tagMap.put("test2Key", "test12Value");
        return tagMap;
    }
    public static void assertThatModelsAreEqual(final Object rawModel,
                                                final Project sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getName()).isEqualTo(sdkModel.name());
    }
}
