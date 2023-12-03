import requests
import os
import json
from datetime import datetime
import time
# from dotenv import load_dotenv 

class GitHubAPIWrapper:


    def __init__(self, base_url="https://api.github.com"):

        # github_token=os.getenv("GITHUB_TOKEN")
        # self.GITHUB_TOKEN = {'authorization': f"token {github_token}"}
        self.GITHUB_API_URL = base_url
        # self.session = requests.Session()
        self.RATE_LIMIT_WAIT_TIME = 10

    def handle_rate_limit(self, response):
        """
        Handles rate limiting by checking the response headers and waiting accordingly.

        Args:
            response (requests.Response): The response object from the API request.

        Returns:
            int or None: The number of seconds to wait for rate limiting, or None if no rate limiting is needed.
        """

        if 'Retry-After' in response.headers:
            retry_after = int(response.headers['Retry-After']) + self.RATE_LIMIT_WAIT_TIME
            print(f"Rate limit exceeded. Waiting for {retry_after} seconds.")
            # time.sleep(retry_after)
            return retry_after
        elif response.headers['x-ratelimit-remaining'] == 0:
            retry_after_in_utc_epoch_seconds = int(response.headers['x-ratelimit-reset'])
            local_datetime = datetime.fromtimestamp(retry_after_in_utc_epoch_seconds)
            reference_date = datetime.now()
            retry_after = (reference_date - local_datetime).total_seconds() + self.RATE_LIMIT_WAIT_TIME
            print(f"Rate limit exceeded. Waiting for {retry_after} seconds.")
            # time.sleep(general_seconds)
            return retry_after
        else:
            return None

    def api_request(self, url, params=None, headers=None):
        """
        Makes an API request to the given URL with optional parameters and headers.

        Args:
            url (str): The URL for the API request.
            params (dict, optional): Optional parameters for the request.
            headers (dict, optional): Optional headers for the request.

        Returns:
            requests.Response or None: The API response or None if the request fails.
        """
        try:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                return response
            elif response.status_code in [403, 429]:
                seconds = self.handle_rate_limit(response)
                if seconds:
                    time.sleep(seconds)
                    return self.api_request(url, params=params, headers=headers)
                else:
                    print("Niether your x-ratelimit-remaining is 0 nor your retry-after is not avalible. inspect or try later.")
                    return None
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch data. {e}")
            return None

    def list_repos(self, organization):
        """
        Retrieves a list of repositories for the given organization.

        Args:
            organization (str): The name of the GitHub organization.
            token (dict): The GitHub authentication token.

        Returns:
            list: A list of repository names.
        """
        url = f"{self.GITHUB_API_URL}/orgs/{organization}/repos"
        github_repos = []

        while True:
            response = self.api_request(url, headers=None) # self.GITHUB_TOKEN
            if response:
                repos = response.json()
                github_repos.extend(repo['name'] for repo in repos)

                # Check for pagination
                if 'next' in response.links:
                    url = response.links['next']['url']
                else:
                    break
            else:
                break

        return github_repos

    def list_prs(self, organization):
        """
        Lists all pull requests for repositories in the given organization.

        Args:
            organization (str): The name of the GitHub organization.
            token (dict): The GitHub authentication token.

        Returns:
            None
        """
        repos = self.list_repos(organization)
        for repo in repos:
            url = f"{self.GITHUB_API_URL}/repos/{organization}/{repo}/pulls"
            params = {'state': 'all'}
            all_prs = []

            while True:
                response = self.api_request(url, params=params, headers=None) # self.GITHUB_TOKEN

                if response:
                    prs = response.json()
                    all_prs.extend(prs)

                    # Check for pagination
                    if 'next' in response.links:
                        url = response.links['next']['url']
                    else:
                        break
                else:
                    break

            print(f"repo: {repo}")
            self.save_prs_to_json(organization, repo, all_prs)
            for pr in all_prs:
                print(f"PR #{pr['number']}: {pr['title']} (State: {pr['state']})")
            print("#############################")

    def save_prs_to_json(self, organization, repo, prs):
        """
        Saves pull requests information to JSON files.

        Args:
            organization (str): The name of the GitHub organization.
            repo (str): The name of the repository.
            prs (list): List of pull requests.

        Returns:
            None
        """
        try:
            directory = os.path.join("../data", f"{organization}_{repo}_prs")
            os.makedirs(directory, exist_ok=True)

            for pr in prs:
                pr_number = pr['number']
                redu_pr = {
                    'Organization Name': pr['head']['repo']['full_name'].split('/')[0], 
                    'repository_id': pr['head']['repo']['id'],
                    'repository_name' : pr['head']['repo']['name'],
                    'repository_owner': pr['head']['repo']['owner']['login'],
                    'state': pr['state'], 
                    'merged_at': self.format_datetime(pr['merged_at']) if 'merged_at' in pr else None,
                    'merged': False if pr['merged_at'] is None else True
                }
            
                file_path = os.path.join(directory, f"pr_{pr_number}.json")

                with open(file_path, 'w') as json_file:
                    json.dump(redu_pr, json_file)

                print(f"PR #{pr_number} saved to {file_path}")
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error while saving pull requests to JSON: {e}")

    def format_datetime(self, timestamp):
        """
        Formats a timestamp into a human-readable and spark accetped date and time.

        Args:
            timestamp (str): The timestamp to be formatted.

        Returns:
            str or None: Formatted date and time or None if the timestamp is None.
        """
        if timestamp:
            dt_object = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
            return dt_object.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None
        
if __name__ == '__main__':

    # load_dotenv()
    github_wrapper = GitHubAPIWrapper()

    organization_name = 'Scytale-exercise'
    github_wrapper.list_prs(organization_name)
    