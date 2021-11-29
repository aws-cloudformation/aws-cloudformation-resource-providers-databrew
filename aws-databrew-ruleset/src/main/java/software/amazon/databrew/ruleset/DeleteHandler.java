package software.amazon.databrew.ruleset;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.*;
import software.amazon.cloudformation.proxy.*;


public class DeleteHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        final String rulesetName = model.getName();
        final DataBrewClient databrewClient = ClientBuilder.getClient();

        final DeleteRulesetRequest deleteRulesetRequest = DeleteRulesetRequest.builder()
                .name(rulesetName)
                .build();

        try {
            proxy.injectCredentialsAndInvokeV2(deleteRulesetRequest, databrewClient::deleteRuleset);
            logger.log(String.format("%s [%s] Deleted Successfully", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, rulesetName));
        } catch (ResourceNotFoundException ex) {
            logger.log(String.format("%s [%s] Does Not Exist", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, rulesetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.NotFound);
        } catch (ValidationException ex) {
            logger.log(String.format("%s [%s] Invalid Parameter", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, rulesetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.InvalidRequest);
        } catch (ConflictException ex) {
            logger.log(String.format("%s [%s] Resource Conflict", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, rulesetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ResourceConflict);
        } catch (DataBrewException ex) {
            logger.log(String.format("%s [%s] Deleted Failed", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, rulesetName));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .status(OperationStatus.SUCCESS)
            .build();
    }
}
