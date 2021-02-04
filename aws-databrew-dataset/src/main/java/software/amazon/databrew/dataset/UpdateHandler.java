package software.amazon.databrew.dataset;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.ResourceNotFoundException;
import software.amazon.awssdk.services.databrew.model.UpdateDatasetRequest;
import software.amazon.awssdk.services.databrew.model.ValidationException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        final String datasetName = model.getName();
        final DataBrewClient databrewClient = ClientBuilder.getClient();

        final UpdateDatasetRequest updateDatasetRequest = UpdateDatasetRequest.builder()
                .name(datasetName)
                .format(model.getFormat())
                .formatOptions(ModelHelper.buildRequestFormatOptions(model.getFormatOptions()))
                .pathOptions(ModelHelper.buildRequestPathOptions(model.getPathOptions()))
                .input(ModelHelper.buildRequestInput(model.getInput()))
                .build();

        try {
            proxy.injectCredentialsAndInvokeV2(updateDatasetRequest, databrewClient::updateDataset);
            logger.log(String.format("%s [%s] Updated Successfully", ResourceModel.TYPE_NAME, datasetName));
        } catch (ResourceNotFoundException ex) {
            logger.log(String.format("%s [%s] Does Not Exist", ResourceModel.TYPE_NAME, datasetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.NotFound);
        } catch (ValidationException ex) {
            logger.log(String.format("%s [%s] Invalid Parameter", ResourceModel.TYPE_NAME, datasetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.InvalidRequest);
        } catch (DataBrewException ex) {
            logger.log(String.format("%s [%s] Updated Failed", ResourceModel.TYPE_NAME, datasetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModel(model)
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
