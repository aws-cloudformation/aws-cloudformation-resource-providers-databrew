# AWS::DataBrew::Recipe

Resource schema for AWS::DataBrew::Recipe.

## Syntax

To declare this entity in your AWS CloudFormation template, use the following syntax:

### JSON

<pre>
{
    "Type" : "AWS::DataBrew::Recipe",
    "Properties" : {
        "<a href="#description" title="Description">Description</a>" : <i>String</i>,
        "<a href="#name" title="Name">Name</a>" : <i>String</i>,
        "<a href="#steps" title="Steps">Steps</a>" : <i>[ <a href="recipestep.md">RecipeStep</a>, ... ]</i>,
        "<a href="#tags" title="Tags">Tags</a>" : <i>[ <a href="tag.md">Tag</a>, ... ]</i>
    }
}
</pre>

### YAML

<pre>
Type: AWS::DataBrew::Recipe
Properties:
    <a href="#description" title="Description">Description</a>: <i>String</i>
    <a href="#name" title="Name">Name</a>: <i>String</i>
    <a href="#steps" title="Steps">Steps</a>: <i>
      - <a href="recipestep.md">RecipeStep</a></i>
    <a href="#tags" title="Tags">Tags</a>: <i>
      - <a href="tag.md">Tag</a></i>
</pre>

## Properties

#### Description

Description of the recipe

_Required_: No

_Type_: String

_Maximum_: <code>1024</code>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Name

Recipe name

_Required_: Yes

_Type_: String

_Minimum_: <code>1</code>

_Maximum_: <code>255</code>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

#### Steps

_Required_: Yes

_Type_: List of <a href="recipestep.md">RecipeStep</a>

_Update requires_: [No interruption](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-no-interrupt)

#### Tags

_Required_: No

_Type_: List of <a href="tag.md">Tag</a>

_Update requires_: [Replacement](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-update-behaviors.html#update-replacement)

## Return Values

### Ref

When you pass the logical ID of this resource to the intrinsic `Ref` function, Ref returns the Name.
