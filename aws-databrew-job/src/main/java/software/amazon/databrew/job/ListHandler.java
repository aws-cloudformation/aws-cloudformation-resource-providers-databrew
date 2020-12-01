package software.amazon.databrew.job;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.ListJobsRequest;
import software.amazon.awssdk.services.databrew.model.ListJobsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.ArrayList;
import java.util.List;

public class ListHandler extends BaseHandler<CallbackContext> {

    private static final int MAX_RESULTS = 100;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel m = request.getDesiredResourceState();
        final DataBrewClient databrewClient = ClientBuilder.getClient();

        final ListJobsRequest listJobsRequest = ListJobsRequest.builder()
                .datasetName(m.getDatasetName())
                .maxResults(MAX_RESULTS)
                .nextToken(request.getNextToken())
                .projectName(m.getProjectName())
                .build();

        final ListJobsResponse listJobsResponse;
        try {
            listJobsResponse = proxy.injectCredentialsAndInvokeV2(listJobsRequest, databrewClient::listJobs);
            logger.log(String.format("%s List Successfully", ResourceModel.TYPE_NAME));

            List<ResourceModel> outputModels = new ArrayList<>();
            if (listJobsResponse.jobs() != null) {
                listJobsResponse.jobs().forEach(job ->{
                    ResourceModel outputModel = ModelHelper.constructModel(job);
                    outputModels.add(outputModel);
                });
            }
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModels(outputModels)
                    .nextToken(request.getNextToken())
                    .status(OperationStatus.SUCCESS)
                    .build();

        } catch (DataBrewException ex) {
            logger.log(String.format("%s List Failed", ResourceModel.TYPE_NAME));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }
    }
}
