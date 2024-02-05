# GitHub Traffic Data: How are our Tools being used?
We're proud to have launched open-source tools to the health tech community! To help us better serve your needs, we welcome feedback in all forms.

One such source of feedback is traffic data. This pivotal data gives us a window into when and how users engage with our tools.

While GitHub offers valuable traffic data via their API, it's only stored for 14 days! To overcome this limitation, we have embarked on an initiative focused on long-term data capture.

Our goal? To gather and preserve traffic data for our repos over time, enabling us to continuously improve engagement and user experience. We plan to use and share these insights, to: 
1. identify the best distribution channels
2. highlight features that deliver the most value to users

To bring this vision to life, we've integrated a robust set of tools to collect and visualize the data.

For MDT (a.k.a. ForgeRx), we're using the following: 
1. GitHub Actions: automates collection of traffic data
2. AWS DynamoDB, Glue, S3: a hub for ELT/ETL and storage
3. PowerBI: for visualizations

# How It Works
1. You need to set up access keys/tokens for AWS & GitHub: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, {GITHUB} PERSONAL_ACCESS_TOKEN. Once you have these, navigate to your repo -> Settings -> Secrets & Variables -> Secrets -> Add New repository secret. 
2. The gh_traffic_to_dynamodb.py script fetches GitHub Traffic data from the GitHub API and writes the output to DynamoDB.
3. Create an Action Worfklow: In your repo, Navigate to Actions -> Select New Workflow -> add gh_action_traffic_scheduled.yml. This schedules the automated workflow to run the pyscript.
4. Most of the steps involving AWS & PowerBI are low-code processes. However, the AWS Glue ETL Job involved a series of transformations in pyspark. This was the most expensive part of the development process since we used the UI, and the lovely UI-generated pyscript is a bonus prize: mdt-glue-etl-job.py. If you're interested in the low-code steps, please visit [this Medium article](https://medium.com/@kristentaytok/how-i-built-it-github-traffic-data-pipeline-01c2e3486a5d) for details! 
