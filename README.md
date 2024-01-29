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
