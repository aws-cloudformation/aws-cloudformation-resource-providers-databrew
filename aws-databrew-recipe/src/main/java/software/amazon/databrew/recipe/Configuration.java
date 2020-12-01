package software.amazon.databrew.recipe;

import java.util.Map;
import org.json.JSONObject;
import org.json.JSONTokener;

class Configuration extends BaseConfiguration {

    public Configuration() {
        super("aws-databrew-recipe.json");
    }
}
