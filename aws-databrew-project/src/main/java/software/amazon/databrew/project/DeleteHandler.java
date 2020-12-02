package software.amazon.databrew.project;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.*;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        final String projectName = model.getName();
        final DataBrewClient databrewClient = ClientBuilder.getClient();

        try {
            final DeleteProjectRequest deleteProjectRequest = DeleteProjectRequest.builder()
                    .name(projectName)
                    .build();
            proxy.injectCredentialsAndInvokeV2(deleteProjectRequest, databrewClient::deleteProject);
            logger.log(String.format("%s [%s] Deleted Successfully", ResourceModel.TYPE_NAME, projectName));
        } catch (ResourceNotFoundException ex) {
            logger.log(String.format("%s [%s] Does Not Exist", ResourceModel.TYPE_NAME, projectName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.NotFound);
        } catch (ValidationException ex) {
            logger.log(String.format("%s [%s] Invalid Parameter", ResourceModel.TYPE_NAME, projectName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.InvalidRequest);
        } catch (DataBrewException ex) {
            logger.log(String.format("%s [%s] Deleted Failed", ResourceModel.TYPE_NAME, projectName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
