package software.amazon.databrew.schedule;

import software.amazon.awssdk.services.databrew.model.DescribeScheduleResponse;
import software.amazon.awssdk.services.databrew.model.Schedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ModelHelper {
    public static ResourceModel constructModel(final DescribeScheduleResponse schedule) {
        Map<String, String> tags = schedule.tags();
        return ResourceModel.builder()
                .jobNames(schedule.jobNames())
                .cronExpression(schedule.cronExpression())
                .name(schedule.name())
                .tags(tags != null ? buildModelTags(tags) : null)
                .build();
    }

    public static ResourceModel constructModel(final Schedule schedule) {
        Map<String, String> tags = schedule.tags();
        return ResourceModel.builder()
                .jobNames(schedule.jobNames())
                .cronExpression(schedule.cronExpression())
                .name(schedule.name())
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
        List<Tag> tagArrayList = new ArrayList<Tag>();
        if (tags == null) return null;
        tags.forEach((k, v) -> tagArrayList.add(Tag.builder().key(k).value(v).build()));
        return tagArrayList;
    }
}
