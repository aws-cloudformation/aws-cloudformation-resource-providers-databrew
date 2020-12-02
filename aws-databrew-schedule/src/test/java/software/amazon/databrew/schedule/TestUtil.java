package software.amazon.databrew.schedule;

import software.amazon.awssdk.services.databrew.model.Schedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtil {

    public static final String SCHEDULE_CRON ="cron(0 0/1 ? * * *)";
    public static final String SCHEDULE_NAME = "schedule-test";
    public static final String INVALID_SCHEDULE_NAME = "invalid-schedule-test";
    public static final List<String> JOB_NAMES = new ArrayList<String>() {{
        add( "job-name-test");
    }};
    public static final Map<String, String> TAGS = new HashMap<String, String>() {{
        put("key", "value");
    }};
    public static void assertThatScheduleModelsAreEqual(final Object rawModel,
                                                final Schedule sdkModel) {
        assertThat(rawModel).isInstanceOf(ResourceModel.class);
        ResourceModel model = (ResourceModel)rawModel;
        assertThat(model.getName()).isEqualTo(sdkModel.name());
    }
}
