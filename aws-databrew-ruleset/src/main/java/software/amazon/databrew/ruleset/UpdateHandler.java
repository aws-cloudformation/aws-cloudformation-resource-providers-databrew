package software.amazon.databrew.ruleset;

import com.google.common.base.Strings;
import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.*;
import software.amazon.cloudformation.proxy.*;

import java.util.List;
import java.util.ArrayList;


public class UpdateHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        final DataBrewClient databrewClient = ClientBuilder.getClient();
        final String name = model.getName();
        final List<Rule> rules = model.getRules();
        if (Strings.isNullOrEmpty(name) || rules == null || rules.size() < 1) {
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .errorCode(HandlerErrorCode.InvalidRequest)
                    .status(OperationStatus.FAILED)
                    .build();
        }
        final UpdateRulesetRequest updateRulesetRequest = UpdateRulesetRequest.builder()
                .name(name)
                .description(model.getDescription())
                .rules(ModelHelper.buildRequestRules(rules))
                .build();

        try {
            proxy.injectCredentialsAndInvokeV2(updateRulesetRequest, databrewClient::updateRuleset);
            model.setTags(model.getTags() == null ? new ArrayList<>() : model.getTags());
            logger.log(String.format("%s [%s] Created Successfully", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, name));
        } catch (ResourceNotFoundException ex) {
            logger.log(String.format("%s [%s] Not found", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, name));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.NotFound);
        } catch (AccessDeniedException ex) {
            logger.log(String.format("%s [%s] Access Denied", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, name));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.AccessDenied);
        } catch (ConflictException ex) {
            logger.log(String.format("%s [%s] Already Exists", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, name));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.AlreadyExists);
        } catch (ValidationException ex) {
            logger.log(String.format("%s [%s] Invalid Parameter", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, name));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.InvalidRequest);
        } catch (ServiceQuotaExceededException ex) {
            logger.log(String.format("%s [%s] Limit Exceeded", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, name));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceLimitExceeded);
        } catch (DataBrewException ex) {
            logger.log(String.format("%s [%s] Created Failed", software.amazon.databrew.ruleset.ResourceModel.TYPE_NAME, name));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }


        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModel(model)
            .status(OperationStatus.SUCCESS)
            .build();
    }
}
