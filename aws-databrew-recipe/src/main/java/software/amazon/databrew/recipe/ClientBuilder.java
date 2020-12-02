package software.amazon.databrew.recipe;

import software.amazon.awssdk.services.databrew.DataBrewClient;
import software.amazon.cloudformation.LambdaWrapper;

public class ClientBuilder {
    public static DataBrewClient getClient() {
        return DataBrewClient.builder().httpClient(LambdaWrapper.HTTP_CLIENT).build();
    }
}
