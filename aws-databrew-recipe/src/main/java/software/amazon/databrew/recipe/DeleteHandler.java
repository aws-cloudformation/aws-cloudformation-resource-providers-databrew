package software.amazon.databrew.recipe;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.DeleteRecipeVersionRequest;
import software.amazon.awssdk.services.databrew.model.ResourceNotFoundException;
import software.amazon.awssdk.services.databrew.model.ValidationException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandler<CallbackContext> {
    private static final String LATEST_WORKING = "LATEST_WORKING";
    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        final String recipeName = model.getName();
        final DataBrewClient databrewClient = ClientBuilder.getClient();

        final DeleteRecipeVersionRequest deleteRecipeVersionRequest = DeleteRecipeVersionRequest.builder()
                .name(recipeName)
                .recipeVersion(LATEST_WORKING)
                .build();

        try {
            proxy.injectCredentialsAndInvokeV2(deleteRecipeVersionRequest, databrewClient::deleteRecipeVersion);
            logger.log(String.format("%s [%s] Deleted Successfully", ResourceModel.TYPE_NAME, recipeName));
        } catch (ResourceNotFoundException ex) {
            logger.log(String.format("%s [%s] Does Not Exist", ResourceModel.TYPE_NAME, recipeName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.NotFound);
        } catch (ValidationException ex) {
            logger.log(String.format("%s [%s] Invalid Parameter", ResourceModel.TYPE_NAME, recipeName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.InvalidRequest);
        } catch (DataBrewException ex) {
            logger.log(String.format("%s [%s] Deleted Failed", ResourceModel.TYPE_NAME, recipeName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
