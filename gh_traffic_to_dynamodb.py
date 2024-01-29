from calendar import c
import boto3
import os
import requests
from datetime import datetime
from collections import defaultdict

# Initialize a DynamoDB client
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name=os.environ['AWS_DEFAULT_REGION']
)

# Reference to your DynamoDB table
table = dynamodb.Table('GH_TRAFFIC_MDT') # Replace with your table name

# GitHub API headers
headers = {'Authorization': f'token {os.environ["MY_PERSONAL_ACCESS_TOKEN"]}'}
repo_name = 'coderxio/medication-diversification'

# Function to safely fetch GitHub Traffic data and handle empty responses
def fetch_github_data(url, is_list=True):
    data = [] if is_list else {}
    while url:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            new_data = response.json()
            if is_list:
                data.extend(new_data)
            else:
                data = new_data
                break  # Break out of loop for single item responses
            url = response.links.get('next', {}).get('url', None)  # Handling pagination
        else:
            print(f"Error fetching data from {url}: {response.status_code}, {response.content}")
            break
    return data

def fetch_and_count(url, count_distinct=False, key='login'):
    data = fetch_github_data(url)
    if count_distinct:
        return len(set(item[key] for item in data if key in item))
    else:
        return len(data)

# Function to count distinct issue submitters
def fetch_and_count_distinct_issue_submitters(url):
    issues = fetch_github_data(url)
    submitters = set()
    timestamps = []
    if isinstance(issues, list):
        for issue in issues:
            if 'user' in issue and 'login' in issue['user']:
                submitters.add(issue['user']['login'])
        return len(submitters)
    else:
        return 0

def process_issues(url, issue_counts):
    issues = fetch_github_data(url)
    for issue in issues:
        open_date = issue['created_at'].split('T')[0]
        issue_counts[open_date]['opened_issues'] += 1
        
        if 'closed_at' in issue and issue['closed_at']:
            close_date = issue['closed_at'].split('T')[0]
            issue_counts[close_date]['closed_issues'] += 1

        if 'comments_url' in issue and issue['comments'] > 0:
            comments = fetch_github_data(issue['comments_url'], is_list=True)
            for comment in comments:
                comment_date = comment['created_at'].split('T')[0]
                issue_counts[comment_date]['comments'] += 1

# Fetch GitHub traffic data
urls = {
    'views': f'https://api.github.com/repos/{repo_name}/traffic/views',
    'clones': f'https://api.github.com/repos/{repo_name}/traffic/clones',
    'referrers': f'https://api.github.com/repos/{repo_name}/traffic/popular/referrers',
    'contents': f'https://api.github.com/repos/{repo_name}/traffic/popular/paths',
    'repo_info': f'https://api.github.com/repos/{repo_name}',
    'watchers': f'https://api.github.com/repos/{repo_name}/subscribers',
    'contributors': f'https://api.github.com/repos/{repo_name}/contributors',
    'open_issues': f'https://api.github.com/repos/{repo_name}/issues?state=open',
    'closed_issues': f'https://api.github.com/repos/{repo_name}/issues?state=closed',
    'issue_submitters': f'https://api.github.com/repos/{repo_name}/issues?state=open'
}

issue_counts = defaultdict(lambda: {'opened_issues': 0, 'closed_issues': 0, 'comments': 0})

# Fetch and process issues
issue_urls = [urls['open_issues'], urls['closed_issues']]
for url in issue_urls:
    process_issues(url, issue_counts)

# Fetch GitHub traffic data with proper handling for single item responses
data = {key: fetch_github_data(url, is_list=key not in ['repo_info', 'views', 'clones']) for key, url in urls.items()}
data['repo_info'] = fetch_github_data(urls['repo_info'], is_list=False)
data['watchers_count'] = fetch_and_count(urls['watchers'])
data['contributors_count'] = fetch_and_count(urls['contributors'])
data['distinct_issue_submitters_count'] = fetch_and_count_distinct_issue_submitters(urls['issue_submitters'])

# Current date and time for the action run
current_datetime = datetime.now().isoformat()

# Formatting the data for DynamoDB
traffic_data = {
    'repo_name': repo_name,
    'action_run_date': current_datetime,
    'views_data': data['views']['views'],
    'clones_data': data['clones']['clones'],
    'referrers': data['referrers'],
    'popular_contents': data['contents'],
    'forks_count': data['repo_info'].get('forks_count', 0),
    'stargazers_count': data['repo_info'].get('stargazers_count', 0),
    'issues_activity_per_date': dict(issue_counts), 
    **{k: data[k] for k in ['watchers_count', 'contributors_count', 'distinct_issue_submitters_count']}
}

# Write data to DynamoDB
table.put_item(Item=traffic_data)
