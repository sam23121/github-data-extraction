# GitHub Data Extraction and PySpark Transformation

This repository contains tools and scripts to extract information about repositories and pull requests from a GitHub organization using the GitHub API. Additionally, it includes a PySpark component for data transformation.

## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Usage](#usage)


## Overview

This home excercise given by Scytale aims to provide a convenient way to retrieve information about repositories and pull requests in the 'Scytale-exercise' organization. The GitHub API is used for this. Also, PySpark transformation is included to process and analyze the collected data.

## Prerequisites

Before using the tools in this repository, ensure you have the following prerequisites:

- Python (version 3.8)
- Spark installed

## Getting Started


1. Clone this repository to your local machine.
   ```
   git clone https://github.com/sam23121/github-data-extraction.git
   ```

2. Install necessary dependencies (preferable if you create a venv)
   ```
   pip install requirements.txt
   ```

3. create an .env for storing your personal github token (Optionally but it is useful to do authenticated requests for getting more handle rate limit as spcified in their [website](https://docs.github.com/en/rest/guides/best-practices-for-using-the-rest-api?apiVersion=2022-11-28))
   ```
   touch .env
   GITHUB_TOKEN=<personal_token>
   ```
   then uncomment and replace your GITHUB_TOKEN on the script/git_api_wrapper.py

4. Run the following code and all the JSON and transformed parquet will be stored in data folder
   ```
   cd scripts
   python git_api_wrapper.py
   python transformation_spark.py
   ```
   
