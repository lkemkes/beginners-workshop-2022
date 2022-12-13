import dlt
import requests
import json
from datetime import datetime, timedelta, timezone


@dlt.source
def twitter_source(search_terms, api_secret_key=dlt.secrets.value, start_time=None, end_time=None):
    return twitter_search(search_terms, start_time=start_time, end_time=end_time, api_secret_key=api_secret_key)


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {
        "Authorization": f"Bearer {api_secret_key}"
    }
    return headers

def _paginated_get(url, headers, params, max_pages=5):
    """Requests and yields up to `max_pages` pages of results as per Twitter API docs: https://developer.twitter.com/en/docs/twitter-api/pagination"""
    while True:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        page = response.json()
        # show the pagination info
        meta = page["meta"]
        print(meta)

        yield page

        # get next page token
        next_token = meta.get('next_token')
        max_pages -= 1

        # if no more pages or we are at the maximum
        if not next_token or max_pages == 0:
            break
        else:
            # set the next_token parameter to get next page
            params['pagination_token'] = next_token

@dlt.resource(write_disposition="append")
def twitter_search(search_terms, start_time=None, end_time=None, api_secret_key=dlt.secrets.value):
    headers = _create_auth_headers(api_secret_key)

    # check if authentication headers look fine
    print(headers)
    url = "https://api.twitter.com/2/tweets/search/recent"

    for search_term in search_terms:
        params = {
            'query': search_term,
            'max_results': 20,
            'start_time': start_time,
            'end_time': end_time,
            'expansions': 'author_id',
            'tweet.fields': 'context_annotations,id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
            'user.fields': 'id,name,username,created_at,description,public_metrics,verified'
            }
        response = _paginated_get(url, headers=headers, params=params)
        for row in response:
            row['search_term'] = params["query"]
            yield row
   



if __name__=='__main__':

    search_terms = ['elon', 'sbf']
    dataset_name = 'tweets'

    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='twitter', destination='bigquery', dataset_name='twitter_data')

    data_interval_start = datetime.now(timezone.utc)- timedelta(days=2)
    data_interval_end = datetime.now(timezone.utc)- timedelta(days=1)
    start_time = data_interval_start.isoformat()
    end_time = data_interval_end.isoformat()

    #data = list(twitter_search(search_terms=search_terms, start_time=start_time, end_time=end_time))
    #print(json.dumps(data[0], indent=4))

    # run the pipeline with your parameters
    load_info = pipeline.run(twitter_source(search_terms=search_terms, start_time=start_time, end_time=end_time))

    # pretty print the information on data that was loaded
    print(load_info)
