package software.amazon.databrew.job;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.databrew.model.Job;
import software.amazon.awssdk.services.databrew.model.S3Location;
import software.amazon.awssdk.services.databrew.model.Output;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtil {

    public static final String JOB_NAME = "job-name";
    public static final String INVALID_JOB_NAME = "invalid-job-name";
    public static final String JOB_TYPE_PROFILE = "PROFILE";
    public static final String JOB_TYPE_RECIPE = "RECIPE";
    public static final String S3_BUCKET = "s3://test-dataset-input-bucket";
    public static final Integer TIMEOUT = 2880;
    public static final Integer UPDATED_TIMEOUT = 2880;
    public static final S3Location S3_LOCATION = S3Location.builder()
            .bucket(S3_BUCKET)
            .build();
    public static final List<Output> OUTPUTS = ImmutableList.of(Output.builder()
            .location(S3_LOCATION).build());
    public static final Map<String, String> sampleTags() {
        Map<String, String> tagMap = new HashMap<>();
        tagMap.put("test1Key", "test1Value");
        tagMap.put("test2Key", "test12Value");
        return tagMap;
    }
    public static void assertThatJobModelsAreEqual(final Object rawModel,
                                                final Job sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getName()).isEqualTo(sdkModel.name());
    }
}
