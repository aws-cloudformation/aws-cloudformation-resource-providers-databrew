package software.amazon.databrew.job;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.ConflictException;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.ResourceNotFoundException;
import software.amazon.awssdk.services.databrew.model.UpdateProfileJobRequest;
import software.amazon.awssdk.services.databrew.model.UpdateRecipeJobRequest;
import software.amazon.awssdk.services.databrew.model.ValidationException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        final DataBrewClient databrewClient = ClientBuilder.getClient();

        final String jobName = model.getName();
        final String jobType = model.getType();

        if ((!jobType.equals(ModelHelper.Type.PROFILE.toString())) && (!jobType.equals(ModelHelper.Type.RECIPE.toString()))) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .errorCode(HandlerErrorCode.InvalidRequest)
                    .status(OperationStatus.FAILED)
                    .build();
        }

        if (jobType.equals(ModelHelper.Type.RECIPE.toString())) {
            final UpdateRecipeJobRequest updateRecipeJobRequest = UpdateRecipeJobRequest.builder()
                    .encryptionKeyArn(model.getEncryptionKeyArn())
                    .encryptionMode(model.getEncryptionMode())
                    .name(jobName)
                    .logSubscription(model.getLogSubscription())
                    .maxCapacity(model.getMaxCapacity())
                    .maxRetries(model.getMaxRetries())
                    .outputs(ModelHelper.buildRequestOutputs(model.getOutputs()))
                    .roleArn(model.getRoleArn())
                    .timeout(model.getTimeout())
                    .build();

            try {
                proxy.injectCredentialsAndInvokeV2(updateRecipeJobRequest, databrewClient::updateRecipeJob);
                logger.log(String.format("%s [%s] Created Successfully", ResourceModel.TYPE_NAME, jobName));
            } catch (ResourceNotFoundException ex) {
                logger.log(String.format("%s [%s] Does Not Exist", ResourceModel.TYPE_NAME, jobName));
                return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.NotFound);
            } catch (ConflictException ex) {
                logger.log(String.format("%s [%s] Already Exists", ResourceModel.TYPE_NAME, jobName));
                return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.AlreadyExists);
            } catch (ValidationException ex) {
                logger.log(String.format("%s [%s] Invalid Parameter", ResourceModel.TYPE_NAME, jobName));
                return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.InvalidRequest);
            } catch (DataBrewException ex) {
                logger.log(String.format("%s Create Failed", ResourceModel.TYPE_NAME));
                return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
            }
        }

        if (jobType.equals(ModelHelper.Type.PROFILE.toString())) {
            final UpdateProfileJobRequest updateProfileJobRequest = UpdateProfileJobRequest.builder()
                    .encryptionKeyArn(model.getEncryptionKeyArn())
                    .encryptionMode(model.getEncryptionMode())
                    .name(jobName)
                    .logSubscription(model.getLogSubscription())
                    .maxCapacity(model.getMaxCapacity())
                    .maxRetries(model.getMaxRetries())
                    .outputLocation(ModelHelper.buildRequestS3Location(model.getOutputLocation()))
                    .roleArn(model.getRoleArn())
                    .timeout(model.getTimeout())
                    .build();

            try {
                proxy.injectCredentialsAndInvokeV2(updateProfileJobRequest, databrewClient::updateProfileJob);
                logger.log(String.format("%s [%s] Created Successfully", ResourceModel.TYPE_NAME, jobName));
            } catch (ResourceNotFoundException ex) {
                logger.log(String.format("%s [%s] Does Not Exist", ResourceModel.TYPE_NAME, jobName));
                return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.NotFound);
            } catch (ConflictException ex) {
                logger.log(String.format("%s [%s] Already Exists", ResourceModel.TYPE_NAME, jobName));
                return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.AlreadyExists);
            } catch (ValidationException ex) {
                logger.log(String.format("%s [%s] Invalid Parameter", ResourceModel.TYPE_NAME, jobName));
                return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.InvalidRequest);
            } catch (DataBrewException ex) {
                logger.log(String.format("%s Create Failed", ResourceModel.TYPE_NAME));
                return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
            }
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModel(model)
                .status(OperationStatus.SUCCESS)
                .build();
    }
}
