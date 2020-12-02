package software.amazon.databrew.project;

import software.amazon.awssdk.services.databrew.model.DescribeProjectResponse;
import software.amazon.awssdk.services.databrew.model.Project;
import software.amazon.awssdk.services.databrew.model.Sample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ModelHelper {
    public static ResourceModel constructModel(final DescribeProjectResponse project) {
        Map<String, String> tags = project.tags();
        return ResourceModel.builder()
                .datasetName(project.datasetName())
                .name(project.name())
                .recipeName(project.recipeName())
                .sample(buildModelSample(project.sample()))
                .roleArn(project.roleArn())
                .tags(tags != null ? buildModelTags(tags) : null)
                .build();
    }

    public static ResourceModel constructModel(final Project project) {
        Map<String, String> tags = project.tags();
        return ResourceModel.builder()
                .datasetName(project.datasetName())
                .name(project.name())
                .recipeName(project.recipeName())
                .sample(buildModelSample(project.sample()))
                .roleArn(project.roleArn())
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

    public static software.amazon.databrew.project.Sample buildModelSample(final Sample requestSample) {
        return requestSample == null ? null : software.amazon.databrew.project.Sample.builder()
                .size(requestSample.size())
                .type(requestSample.typeAsString())
                .build();
    }

    public static Sample buildRequestSample(final software.amazon.databrew.project.Sample modelSample) {
        return modelSample == null ? null : Sample.builder()
                .size(modelSample.getSize())
                .type(modelSample.getType())
                .build();
    }

    public static List<Tag> buildModelTags(final Map<String, String> tags) {
        List<Tag> tagArrayList = new ArrayList<>();
        if (tags == null) return null;
        tags.forEach((k, v) -> tagArrayList.add(Tag.builder().key(k).value(v).build()));
        return tagArrayList;
    }
}
