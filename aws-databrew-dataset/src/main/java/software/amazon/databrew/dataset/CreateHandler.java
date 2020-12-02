package software.amazon.databrew.dataset;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.AccessDeniedException;
import software.amazon.awssdk.services.databrew.model.ConflictException;
import software.amazon.awssdk.services.databrew.model.CreateDatasetRequest;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.ServiceQuotaExceededException;
import software.amazon.awssdk.services.databrew.model.ValidationException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.ArrayList;

public class CreateHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        final String datasetName = model.getName();
        final DataBrewClient databrewClient = ClientBuilder.getClient();
        final CreateDatasetRequest createDatasetRequest = CreateDatasetRequest.builder()
                .name(datasetName)
                .formatOptions(ModelHelper.buildRequestFormatOptions(model.getFormatOptions()))
                .input(ModelHelper.buildRequestInput(model.getInput()))
                .tags(ModelHelper.buildTagInputMap(model.getTags()))
                .build();

        try {
            proxy.injectCredentialsAndInvokeV2(createDatasetRequest, databrewClient::createDataset);
            model.setTags(model.getTags() == null ? new ArrayList<>() : model.getTags());
            logger.log(String.format("%s [%s] Created Successfully", ResourceModel.TYPE_NAME, datasetName));
        } catch (AccessDeniedException ex) {
            logger.log(String.format("%s [%s] Access Denied", ResourceModel.TYPE_NAME, datasetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.AccessDenied);
        } catch (ConflictException ex) {
            logger.log(String.format("%s [%s] Already Exists", ResourceModel.TYPE_NAME, datasetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.AlreadyExists);
        } catch (ValidationException ex) {
            logger.log(String.format("%s [%s] Invalid Parameter", ResourceModel.TYPE_NAME, datasetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.InvalidRequest);
        } catch (ServiceQuotaExceededException ex) {
            logger.log(String.format("%s [%s] Limit Exceeded", ResourceModel.TYPE_NAME, datasetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceLimitExceeded);
        } catch (DataBrewException ex) {
            logger.log(String.format("%s [%s] Created Failed", ResourceModel.TYPE_NAME, datasetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }
        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModel(model)
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
