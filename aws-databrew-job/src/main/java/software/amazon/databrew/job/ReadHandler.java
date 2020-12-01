package software.amazon.databrew.job;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.DescribeJobRequest;
import software.amazon.awssdk.services.databrew.model.DescribeJobResponse;
import software.amazon.awssdk.services.databrew.model.ResourceNotFoundException;
import software.amazon.awssdk.services.databrew.model.ValidationException;
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

        final String jobName = model.getName();
        final DataBrewClient databrewClient = ClientBuilder.getClient();
        final DescribeJobRequest describeJobRequest = DescribeJobRequest.builder()
                .name(jobName)
                .build();

        final DescribeJobResponse describeJobResponse;
        try {
            describeJobResponse = proxy.injectCredentialsAndInvokeV2(describeJobRequest, databrewClient::describeJob);
            logger.log(String.format("%s [%s] Read Successfully", ResourceModel.TYPE_NAME, jobName));
            ResourceModel resultModel = ModelHelper.constructModel(describeJobResponse);
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModel(resultModel)
                    .status(OperationStatus.SUCCESS)
                    .build();

        } catch (ResourceNotFoundException ex) {
            logger.log(String.format("%s [%s] Does Not Exist", ResourceModel.TYPE_NAME, jobName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.NotFound);
        } catch (ValidationException ex) {
            logger.log(String.format("%s [%s] Invalid Parameter", ResourceModel.TYPE_NAME, jobName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.InvalidRequest);
        } catch (DataBrewException ex) {
            logger.log(String.format("%s [%s] Read Failed", ResourceModel.TYPE_NAME, jobName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }
    }
}
