import unittest
from unittest import mock
import tap_twitter_ads

class MockParseArgs:
    '''Mock the parsed_args() in main'''
    config = {}
    state = {}
    catalog = {}
    discover = False

    def __init__(self, config, state, catalog, discover):
        self.config = config
        self.state = state
        self.catalog = catalog
        self.discover = discover

def get_args(config, state, catalog, discover):
    '''Return the MockParseArgs object'''
    return MockParseArgs(config, state, catalog, discover)

@mock.patch("tap_twitter_ads.Client")
@mock.patch("singer.utils.parse_args")
class TestTimeoutValue(unittest.TestCase):
    '''
    Test that request timeout parameter works properly in various cases
    '''

    def test_timeout_value_in_config(self, mocked_parse_args, mocked_client):
        """ 
            Unit tests to ensure that request timeout is set based on config value
        """
        mock_config = {'start_date': 'test_start_date',
                  'consumer_key': 'test_ck',
                  'consumer_secret': 'test_ck',
                  'access_token': 'test_at',
                  'access_token_secret': 'test_ts',
                  'account_ids': 'test_acc_ids',
                  'request_timeout': 100}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config, {}, {}, False)

        # function call
        tap_twitter_ads.main()
        # verify that the request was called with expected timeout value
        mocked_client.assert_called_with(consumer_key='test_ck', consumer_secret='test_ck', access_token='test_at', access_token_secret='test_ts', options={'handle_rate_limit': True, 'retry_max': 10, 'retry_delay': 60000, 'retry_on_status': [400, 420, 500, 502, 503, 504], 'retry_on_timeouts': True, 'timeout': (100.0, 100.0)})

    def test_timeout_value_not_in_config(self, mocked_parse_args, mocked_client):
        """ 
            Unit tests to ensure that request timeout is set based default value
        """
        mock_config = {'start_date': 'test_start_date',
                  'consumer_key': 'test_ck',
                  'consumer_secret': 'test_ck',
                  'access_token': 'test_at',
                  'access_token_secret': 'test_ts',
                  'account_ids': 'test_acc_ids'}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config, {}, {}, False)

        # function call
        tap_twitter_ads.main()
        # verify that the request was called with expected timeout value
        mocked_client.assert_called_with(consumer_key='test_ck', consumer_secret='test_ck', access_token='test_at', access_token_secret='test_ts', options={'handle_rate_limit': True, 'retry_max': 10, 'retry_delay': 60000, 'retry_on_status': [400, 420, 500, 502, 503, 504], 'retry_on_timeouts': True, 'timeout': (300, 300)})
    
    def test_timeout_string_value_in_config(self, mocked_parse_args, mocked_client):
        """ 
            Unit tests to ensure that request timeout is set based on config if string value is given in config
        """
        mock_config = {'start_date': 'test_start_date',
                  'consumer_key': 'test_ck',
                  'consumer_secret': 'test_ck',
                  'access_token': 'test_at',
                  'access_token_secret': 'test_ts',
                  'account_ids': 'test_acc_ids',
                  'request_timeout': '100'}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config, {}, {}, False)

        # function call
        tap_twitter_ads.main()
        # verify that the request was called with expected timeout value
        mocked_client.assert_called_with(consumer_key='test_ck', consumer_secret='test_ck', access_token='test_at', access_token_secret='test_ts', options={'handle_rate_limit': True, 'retry_max': 10, 'retry_delay': 60000, 'retry_on_status': [400, 420, 500, 502, 503, 504], 'retry_on_timeouts': True, 'timeout': (100.0, 100.0)})
    
    def test_timeout_empty_value_in_config(self, mocked_parse_args, mocked_client):
        """ 
            Unit tests to ensure that request timeout is set based on default value if empty value is given in config
        """
        mock_config = {'start_date': 'test_start_date',
                  'consumer_key': 'test_ck',
                  'consumer_secret': 'test_ck',
                  'access_token': 'test_at',
                  'access_token_secret': 'test_ts',
                  'account_ids': 'test_acc_ids',
                  'request_timeout': ''}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config, {}, {}, False)

        # function call
        tap_twitter_ads.main()
        # verify that the request was called with expected timeout value
        mocked_client.assert_called_with(consumer_key='test_ck', consumer_secret='test_ck', access_token='test_at', access_token_secret='test_ts', options={'handle_rate_limit': True, 'retry_max': 10, 'retry_delay': 60000, 'retry_on_status': [400, 420, 500, 502, 503, 504], 'retry_on_timeouts': True, 'timeout': (300, 300)})
    
    def test_timeout_0_value_in_config(self, mocked_parse_args, mocked_client):
        """ 
            Unit tests to ensure that request timeout is set based on default value if 0 is given in config
        """
        mock_config = {'start_date': 'test_start_date',
                  'consumer_key': 'test_ck',
                  'consumer_secret': 'test_ck',
                  'access_token': 'test_at',
                  'access_token_secret': 'test_ts',
                  'account_ids': 'test_acc_ids',
                  'request_timeout': 0}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config, {}, {}, False)

        # function call
        tap_twitter_ads.main()
        # verify that the request was called with expected timeout value
        mocked_client.assert_called_with(consumer_key='test_ck', consumer_secret='test_ck', access_token='test_at', access_token_secret='test_ts', options={'handle_rate_limit': True, 'retry_max': 10, 'retry_delay': 60000, 'retry_on_status': [400, 420, 500, 502, 503, 504], 'retry_on_timeouts': True, 'timeout': (300, 300)})
    
    def test_timeout_string_0_value_in_config(self, mocked_parse_args, mocked_client):
        """ 
            Unit tests to ensure that request timeout is set based on default value if string 0  is given in config
        """
        mock_config = {'start_date': 'test_start_date',
                  'consumer_key': 'test_ck',
                  'consumer_secret': 'test_ck',
                  'access_token': 'test_at',
                  'access_token_secret': 'test_ts',
                  'account_ids': 'test_acc_ids',
                  'request_timeout': "0"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config, {}, {}, False)

        # function call
        tap_twitter_ads.main()
        # verify that the request was called with expected timeout value
        mocked_client.assert_called_with(consumer_key='test_ck', consumer_secret='test_ck', access_token='test_at', access_token_secret='test_ts', options={'handle_rate_limit': True, 'retry_max': 10, 'retry_delay': 60000, 'retry_on_status': [400, 420, 500, 502, 503, 504], 'retry_on_timeouts': True, 'timeout': (300, 300)})

    def test_timeout_float_value_in_config(self, mocked_parse_args, mocked_client):
        """ 
            Unit tests to ensure that request timeout is set based on config float value
        """
        mock_config = {'start_date': 'test_start_date',
                  'consumer_key': 'test_ck',
                  'consumer_secret': 'test_ck',
                  'access_token': 'test_at',
                  'access_token_secret': 'test_ts',
                  'account_ids': 'test_acc_ids',
                  'request_timeout': 100.8}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config, {}, {}, False)

        # function call
        tap_twitter_ads.main()
        # verify that the request was called with expected timeout value
        mocked_client.assert_called_with(consumer_key='test_ck', consumer_secret='test_ck', access_token='test_at', access_token_secret='test_ts', options={'handle_rate_limit': True, 'retry_max': 10, 'retry_delay': 60000, 'retry_on_status': [400, 420, 500, 502, 503, 504], 'retry_on_timeouts': True, 'timeout': (100.8, 100.8)})
