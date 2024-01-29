import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_flatten
from awsglue.dynamicframe import DynamicFrameCollection
import gs_uuid
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
import gs_explode
from pyspark.sql import functions as SqlFuncs


# Script generated for node Custom Transform - Rename Issue Cols
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, explode, map_keys
    from pyspark.sql.functions import (
        lit,
        first,
        concat_ws,
        regexp_replace,
        to_date,
        when,
        expr,
    )
    import pyspark.sql.functions as F

    def flatten_struct(df, struct_col_name):
        # Check if the struct column exists in the DataFrame
        if struct_col_name not in df.columns:
            raise ValueError(
                f"Column {struct_col_name} does not exist in the DataFrame"
            )

        # Iterate over fields in the struct
        for field in df.schema[struct_col_name].dataType.fields:
            date = field.name
            # Create columns for each metric within the date
            for subfield in field.dataType.fields:
                metric = subfield.name
                df = df.withColumn(
                    f"{struct_col_name}_{date.replace('-','')}_{metric.replace('_issues','issues')}",
                    col(f"{struct_col_name}.{date}.{metric}"),
                )

        # Drop the original struct column if no longer needed
        df = df.drop(struct_col_name)
        return df

    # Select the input DynamicFrame
    input_dyf = dfc.select(
        "AmazonDynamoDB_node1705335440488"
    )  # Replace 'input_df' with your actual DynamicFrame name
    input_df = input_dyf.toDF()

    # Flatten the struct column 'issues_activity_per_date'
    flattened_df = flatten_struct(input_df, "issues_activity_per_date")

    # Construct the expression for unpivot
    unpivot_exprs = []
    sub_cols_to_drop = []
    for col_name in flattened_df.columns:
        if col_name.startswith("issues_activity_per_date_"):
            parts = col_name.split("_")
            date = parts[-2]  # Extract the date (YYYY-MM-DD)
            metric = parts[
                -1
            ]  # Extract the metric type (comments, closed_issues, or opened_issues)
            unpivot_exprs.append(f"'{date}', '{metric}', {col_name}")

            # Add the sub-column name to the list for dropping later
            # new_col_name = f"{date}_{metric}"
            # flattened_df = flattened_df.drop(new_col_name)
            sub_cols_to_drop.append(col_name)

    # Check if unpivot_exprs is not empty
    if not unpivot_exprs:
        raise ValueError(
            "No columns found for unpivoting. Please check the column naming convention."
            + ",".join(flattened_df.columns)
        )

    # Apply the unpivot expression
    stack_expr = f"stack({len(unpivot_exprs)}, {', '.join(unpivot_exprs)}) as (IssueDate, MetricName, MetricValue)"
    unpivoted_df = flattened_df.selectExpr("*", stack_expr)

    # # Drop the original struct column and all its sub-columns
    unpivoted_df = unpivoted_df.drop(*sub_cols_to_drop)

    # if len(unpivoted_df.columns) > 5:
    #     raise ValueError(unpivoted_df.columns)

    # Convert the transformed DataFrame back to DynamicFrame
    output_dyf = DynamicFrame.fromDF(unpivoted_df, glueContext, "unpivoted_dyf")

    # # Return a DynamicFrameCollection
    # return DynamicFrameCollection({"output_dyf": output_dyf}, glueContext)

    # Convert DynamicFrame to DataFrame for further transformations
    df = output_dyf.toDF()

    # Convert IssueDate from 'YYYYMMDD' to 'YYYY-MM-DD' format
    input_df = df.withColumn(
        "IssueDate",
        expr(
            "concat(substring(IssueDate, 1, 4), '-', substring(IssueDate, 5, 2), '-', substring(IssueDate, 7, 2))"
        ),
    )

    # Pivot the data
    pivot_df = (
        input_df.groupBy("IssueDate").pivot("MetricName").agg(F.first("MetricValue"))
    )

    # Join the pivoted data with the original data
    original_columns = [
        col for col in input_df.columns if col not in ["MetricName", "MetricValue"]
    ]
    result_df = (
        input_df.select(*original_columns)
        .join(pivot_df, "IssueDate", "inner")
        .dropDuplicates(["action_run_date", "IssueDate"])
    )

    # Convert the transformed pivoted DataFrame back to DynamicFrame
    final_output_dyf = DynamicFrame.fromDF(result_df, glueContext, "final_output_dyf")

    # Return a DynamicFrameCollection
    return DynamicFrameCollection({"final_output_dyf": final_output_dyf}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon DynamoDB
AmazonDynamoDB_node1705335440488 = glueContext.create_dynamic_frame.from_catalog(
    database="mdt_glue_db",
    table_name="gh_traffic_mdt",
    transformation_ctx="AmazonDynamoDB_node1705335440488",
)

# Script generated for node Custom Transform - Rename Issue Cols
CustomTransformRenameIssueCols_node1705363666839 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"AmazonDynamoDB_node1705335440488": AmazonDynamoDB_node1705335440488},
        glueContext,
    ),
)

# Script generated for node Select From Collection - Post Custom Transform
SelectFromCollectionPostCustomTransform_node1705399294423 = SelectFromCollection.apply(
    dfc=CustomTransformRenameIssueCols_node1705363666839,
    key=list(CustomTransformRenameIssueCols_node1705363666839.keys())[0],
    transformation_ctx="SelectFromCollectionPostCustomTransform_node1705399294423",
)

# Script generated for node Change Schema - rename
ChangeSchemarename_node1705400007431 = ApplyMapping.apply(
    frame=SelectFromCollectionPostCustomTransform_node1705399294423,
    mappings=[
        ("IssueDate", "string", "issue_date", "date"),
        ("clones_data", "array", "clones_data", "array"),
        ("watchers_count", "bigint", "watchers_count", "long"),
        ("views_data", "array", "views_data", "array"),
        ("stargazers_count", "bigint", "stargazers_count", "long"),
        ("repo_name", "string", "repo_name", "string"),
        ("contributors_count", "bigint", "contributors_count", "long"),
        ("action_run_date", "string", "action_run_date", "timestamp"),
        ("forks_count", "bigint", "forks_count", "long"),
        (
            "distinct_issue_submitters_count",
            "bigint",
            "distinct_issue_submitters_count",
            "long",
        ),
        ("referrers", "array", "referrers", "array"),
        ("popular_contents", "array", "popular_contents", "array"),
        ("closedissues", "bigint", "closedissues", "long"),
        ("comments", "bigint", "comments", "long"),
        ("openedissues", "bigint", "openedissues", "long"),
    ],
    transformation_ctx="ChangeSchemarename_node1705400007431",
)

# Script generated for node Explode Array Or Map Into Rows - Popular Contents
ExplodeArrayOrMapIntoRowsPopularContents_node1705362196014 = (
    ChangeSchemarename_node1705400007431.gs_explode(
        colName="popular_contents", newCol="popular_content"
    )
)

# Script generated for node Explode Array Or Map Into Rows - Referrers
ExplodeArrayOrMapIntoRowsReferrers_node1705335901013 = (
    ExplodeArrayOrMapIntoRowsPopularContents_node1705362196014.gs_explode(
        colName="referrers", newCol="referrer"
    )
)

# Script generated for node Explode Array Or Map Into Rows - Clones
ExplodeArrayOrMapIntoRowsClones_node1705361690648 = (
    ExplodeArrayOrMapIntoRowsReferrers_node1705335901013.gs_explode(
        colName="clones_data", newCol="clone"
    )
)

# Script generated for node Explode Array Or Map Into Rows - Views
ExplodeArrayOrMapIntoRowsViews_node1705362027190 = (
    ExplodeArrayOrMapIntoRowsClones_node1705361690648.gs_explode(
        colName="views_data", newCol="views"
    )
)

# Script generated for node Change Schema
ChangeSchema_node1705359494816 = ApplyMapping.apply(
    frame=ExplodeArrayOrMapIntoRowsViews_node1705362027190,
    mappings=[
        ("issue_date", "date", "issue_date", "date"),
        ("watchers_count", "bigint", "watchers_count", "long"),
        ("stargazers_count", "bigint", "stargazers_count", "long"),
        ("repo_name", "string", "repo_name", "string"),
        ("contributors_count", "bigint", "contributors_count", "long"),
        ("action_run_date", "timestamp", "action_run_date", "timestamp"),
        ("forks_count", "bigint", "forks_count", "long"),
        (
            "distinct_issue_submitters_count",
            "bigint",
            "distinct_issue_submitters_count",
            "long",
        ),
        ("closedissues", "bigint", "closedissues", "long"),
        ("comments", "bigint", "comments", "bigint"),
        ("openedissues", "bigint", "openedissues", "long"),
        ("popular_content.path", "string", "popular_content.path", "string"),
        ("popular_content.count", "bigint", "popular_content.count", "long"),
        ("popular_content.uniques", "bigint", "popular_content.uniques", "long"),
        ("popular_content.title", "string", "popular_content.title", "string"),
        ("referrer.referrer", "string", "referrer.referrer", "string"),
        ("referrer.count", "bigint", "referrer.count", "bigint"),
        ("referrer.uniques", "bigint", "referrer.uniques", "bigint"),
        ("clone.count", "bigint", "clone.count", "bigint"),
        ("clone.uniques", "bigint", "clone.uniques", "bigint"),
        ("clone.timestamp", "string", "clone.timestamp", "string"),
        ("views.count", "bigint", "views.count", "bigint"),
        ("views.uniques", "bigint", "views.uniques", "bigint"),
        ("views.timestamp", "string", "views.timestamp", "string"),
    ],
    transformation_ctx="ChangeSchema_node1705359494816",
)

# Script generated for node Flatten
Flatten_node1706020819734 = ChangeSchema_node1705359494816.gs_flatten(
    maxLevels=1, separator="_"
)

# Script generated for node Drop Duplicates
DropDuplicates_node1705335927232 = DynamicFrame.fromDF(
    Flatten_node1706020819734.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1705335927232",
)

# Script generated for node UUID
UUID_node1705335944714 = DropDuplicates_node1705335927232.gs_uuid()

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1705335976360_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
     UniqueValueRatio "action_run_date" > 0.9, ReferentialIntegrity "action_run_date,repo_name" "AmazonDynamoDB_node1705335440488.{action_run_date,repo_name}" = 1.0, DetectAnomalies "RowCount",
         DataFreshness "current_timestamp().from_utc_timestamp(current_timestamp_utc, 'America/Los_Angeles')
    -action_run_date" < 30 hours
    ]
"""

EvaluateDataQuality_node1705335976360 = EvaluateDataQuality().process_rows(
    frame=UUID_node1705335944714,
    ruleset=EvaluateDataQuality_node1705335976360_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1705335976360",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "observations.scope": "ALL",
        "performanceTuning.caching": "CACHE_NOTHING",
    },
)

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1705676903412 = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node1705335976360,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node1705676903412",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1705678557596 = glueContext.write_dynamic_frame.from_catalog(
    frame=rowLevelOutcomes_node1705676903412,
    database="mdt_glue_db",
    table_name="etl-table-glue",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="AWSGlueDataCatalog_node1705678557596",
)

# Script generated for node Amazon S3
AmazonS3_node1705655628239 = glueContext.write_dynamic_frame.from_options(
    frame=rowLevelOutcomes_node1705676903412,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://mdt-bucket-kt2/mdt-gh-action-data3/",
        "partitionKeys": ["action_run_date"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1705655628239",
)

job.commit()
