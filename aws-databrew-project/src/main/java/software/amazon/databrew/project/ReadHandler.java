package software.amazon.databrew.project;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.DescribeProjectRequest;
import software.amazon.awssdk.services.databrew.model.DescribeProjectResponse;
import software.amazon.awssdk.services.databrew.model.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        final String projectName = model.getName();
        final DataBrewClient databrewClient = ClientBuilder.getClient();
        final DescribeProjectRequest describeProjectRequest = DescribeProjectRequest.builder()
                .name(projectName)
                .build();

        final DescribeProjectResponse describeProjectResponse;
        try {
            describeProjectResponse = proxy.injectCredentialsAndInvokeV2(describeProjectRequest, databrewClient::describeProject);
            logger.log(String.format("%s [%s] Read Successfully", ResourceModel.TYPE_NAME, projectName));
            ResourceModel resultModel = ModelHelper.constructModel(describeProjectResponse);
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(resultModel)
                    .status(OperationStatus.SUCCESS)
                    .build();
        } catch (ResourceNotFoundException ex) {
            logger.log(String.format("%s [%s] Does Not Exist", ResourceModel.TYPE_NAME, projectName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.NotFound);
        } catch (DataBrewException ex) {
            logger.log(String.format("%s [%s] Read Failed", ResourceModel.TYPE_NAME, projectName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }
    }
}
