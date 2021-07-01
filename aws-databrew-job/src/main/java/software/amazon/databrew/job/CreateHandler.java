package software.amazon.databrew.job;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.ConflictException;
import software.amazon.awssdk.services.databrew.model.CreateProfileJobRequest;
import software.amazon.awssdk.services.databrew.model.CreateRecipeJobRequest;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.ResourceNotFoundException;
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
        final DataBrewClient databrewClient = ClientBuilder.getClient();

        final String jobName = model.getName();
        final String jobType = model.getType();
        final JobSample jobSample = model.getJobSample();

        if (((!jobType.equals(ModelHelper.Type.PROFILE.toString())) && (!jobType.equals(ModelHelper.Type.RECIPE.toString()))) ||
            jobType.equals(ModelHelper.Type.RECIPE.toString()) && jobSample != null) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .errorCode(HandlerErrorCode.InvalidRequest)
                    .status(OperationStatus.FAILED)
                    .build();
        }


        if (jobType.equals(ModelHelper.Type.RECIPE.toString())) {
            final CreateRecipeJobRequest createRecipeJobRequest = CreateRecipeJobRequest.builder()
                    .datasetName(model.getDatasetName())
                    .encryptionKeyArn(model.getEncryptionKeyArn())
                    .encryptionMode(model.getEncryptionMode())
                    .name(jobName)
                    .logSubscription(model.getLogSubscription())
                    .maxCapacity(model.getMaxCapacity())
                    .maxRetries(model.getMaxRetries())
                    .outputs(ModelHelper.buildRequestOutputs(model.getOutputs()))
                    .dataCatalogOutputs(ModelHelper.buildRequestDataCatalogOutputs(model.getDataCatalogOutputs()))
                    .projectName(model.getProjectName())
                    .recipeReference(ModelHelper.buildRequestRecipe(model.getRecipe()))
                    .roleArn(model.getRoleArn())
                    .tags(ModelHelper.buildTagInputMap(model.getTags()))
                    .timeout(model.getTimeout())
                    .build();

            try {
                proxy.injectCredentialsAndInvokeV2(createRecipeJobRequest, databrewClient::createRecipeJob);
                model.setTags(model.getTags() == null ? new ArrayList<>() : model.getTags());
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
        else if (jobType.equals(ModelHelper.Type.PROFILE.toString())) {
            final CreateProfileJobRequest createProfileJobRequest = CreateProfileJobRequest.builder()
                    .datasetName(model.getDatasetName())
                    .encryptionKeyArn(model.getEncryptionKeyArn())
                    .encryptionMode(model.getEncryptionMode())
                    .name(jobName)
                    .logSubscription(model.getLogSubscription())
                    .maxCapacity(model.getMaxCapacity())
                    .maxRetries(model.getMaxRetries())
                    .outputLocation(ModelHelper.buildRequestS3Location(model.getOutputLocation()))
                    .roleArn(model.getRoleArn())
                    .tags(ModelHelper.buildTagInputMap(model.getTags()))
                    .timeout(model.getTimeout())
                    .jobSample(ModelHelper.buildModelJobSample(model.getJobSample()))
                    .build();

            try {
                proxy.injectCredentialsAndInvokeV2(createProfileJobRequest, databrewClient::createProfileJob);
                model.setTags(model.getTags() == null ? new ArrayList<>() : model.getTags());
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
