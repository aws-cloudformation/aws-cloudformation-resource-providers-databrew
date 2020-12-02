package software.amazon.databrew.recipe;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.awssdk.services.databrew.model.DataBrewException;
import software.amazon.awssdk.services.databrew.model.ListRecipesRequest;
import software.amazon.awssdk.services.databrew.model.ListRecipesResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.HandlerErrorCode;

import java.util.ArrayList;
import java.util.List;

public class ListHandler extends BaseHandler<CallbackContext> {
    private static final String LATEST_WORKING = "LATEST_WORKING";
    private static final int MAX_RESULTS = 100;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel m = request.getDesiredResourceState();
        final DataBrewClient databrewClient = ClientBuilder.getClient();

        final ListRecipesRequest listRecipesRequest = ListRecipesRequest.builder()
                .maxResults(MAX_RESULTS)
                .nextToken(request.getNextToken())
                .recipeVersion(LATEST_WORKING)
                .build();

        final ListRecipesResponse listRecipesResponse;
        try {
            listRecipesResponse = proxy.injectCredentialsAndInvokeV2(listRecipesRequest, databrewClient::listRecipes);
            List<ResourceModel> outputModels = new ArrayList<>();

            if (listRecipesResponse.recipes() != null){
                listRecipesResponse.recipes().forEach(recipe ->{
                    ResourceModel outputModel = ModelHelper.constructModel(recipe);
                    outputModels.add(outputModel);
                });
            }

            logger.log(String.format("%s List Successfully", ResourceModel.TYPE_NAME));
            return ProgressEvent.<ResourceModel, CallbackContext>builder()
                    .resourceModels(outputModels)
                    .nextToken(request.getNextToken())
                    .status(OperationStatus.SUCCESS)
                    .build();

        } catch (DataBrewException ex) {
            logger.log(String.format("%s List Failed", ResourceModel.TYPE_NAME));
            return ProgressEvent.defaultFailureHandler(ex, HandlerErrorCode.ServiceInternalError);
        }
    }
}
