import dlt
import requests


@dlt.source
def twitter_source(api_secret_key=dlt.secrets.value):
    return twitter_resource(api_secret_key)


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {
        "Authorization": f"Bearer {api_secret_key}"
    }
    return headers


@dlt.resource(write_disposition="append")
def twitter_resource(api_secret_key=dlt.secrets.value):
    headers = _create_auth_headers(api_secret_key)

    # check if authentication headers look fine
    print(headers)

    # make an api call here
    url = "https://api.twitter.com/2/tweets/search/recent"
    params = {"query": "elon"}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    yield response.json()


if __name__=='__main__':
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(pipeline_name='twitter', destination='bigquery', dataset_name='twitter_data')

    # print credentials by running the resource
    data = list(twitter_resource())

    # print the data yielded from resource
    print(data)
    exit()

    # run the pipeline with your parameters
    load_info = pipeline.run(twitter_source())

    # pretty print the information on data that was loaded
    print(load_info)
