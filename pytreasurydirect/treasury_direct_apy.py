import datetime
import itertools
import datetime
import csv
import requests
import io
import functools

from concurrent import futures


class TDException(Exception):
    def __init__(self, error):
        self.error = error

    def __str__(self):
        return self.error


error_400 = TDException('Bad request')
error_401 = TDException('Unauthorized')
error_404 = TDException('Treasury data not found')
error_429 = TDException('Too many requests')
error_500 = TDException('Internal server error')
error_503 = TDException('Service unavailable')

def sort_by_aggregation_normalization(key_tuple):
    '''
    For the numerous sort_by_<time_aggregation>* functions
    defined within this lib, takes their output of
    tuples of int (), and converts it into a string
    of the following format:

    (2018, 10, 1) -> 2018-10-1
    (2018, 49) -> 2018-49
    '''
    return '-'.join(map(str, key_tuple))

def sort_by_date_base(issue, attribute=None):
    '''
    Retreives the attribute specified from issue 
    and returns a tuple of int for sorting purposes
    '''
    attribute_date = issue[attribute]
    return (attribute_date.year, attribute_date.month, attribute_date.day)

def sort_by_week_base(issue, attribute=None):
    '''
    Retreives the attribute specified from issue 
    and returns a tuple of int for sorting purposes
    '''
    attribute_week = issue[attribute]
    return (attribute_week.year, divmod(attribute_week.timetuple().tm_yday, 7)[0] + 1)

def sort_by_month_base(issue, attribute=None):
    '''
    Retreives the attribute specified from issue 
    and returns a tuple of int for sorting purposes
    '''
    attribute_month = issue[attribute]
    return (attribute_month.year, attribute_month.month)

# Maturity date based sorting functions
sort_by_date_maturity = functools.partial(sort_by_date_base, attribute='actual_maturity_pyobj')
sort_by_week_maturity = functools.partial(sort_by_week_base, attribute='actual_maturity_pyobj')
sort_by_month_maturity = functools.partial(sort_by_month_base, attribute='actual_maturity_pyobj')

# Issue date based sorting functions
sort_by_date_issued = functools.partial(sort_by_date_base, attribute='issueDate_pyobj')
sort_by_week_issued = functools.partial(sort_by_week_base, attribute='issueDate_pyobj')
sort_by_month_issued = functools.partial(sort_by_month_base, attribute='issueDate_pyobj')

# Auctioned date based sorting functions
sort_by_date_auctioned = functools.partial(sort_by_date_base, attribute='auctionDate_pyobj')
sort_by_week_auctioned = functools.partial(sort_by_week_base, attribute='auctionDate_pyobj')
sort_by_month_auctioned = functools.partial(sort_by_month_base, attribute='auctionDate_pyobj')

# Announced date based sorting functions
sort_by_date_announced = functools.partial(sort_by_date_base, attribute='announcementDate_pyobj')
sort_by_week_announced = functools.partial(sort_by_week_base, attribute='announcementDate_pyobj')
sort_by_month_announced = functools.partial(sort_by_month_base, attribute='announcementDate_pyobj')

# Sort by security type 
def sort_by_term(issue):
    return issue['securityTerm']


class TreasuryFedBase:

    def _raise_status(self, response):
        if response.status_code == 400:
            raise error_400
        elif response.status_code == 401:
            raise error_401
        elif response.status_code == 404:
            raise error_404
        elif response.status_code == 429:
            raise error_429
        elif response.status_code == 500:
            raise error_500
        elif response.status_code == 503:
            raise error_503
        else:
            response.raise_for_status()

    def _check_cusip(self, cusip):
        if len(cusip) != 9:
            raise Exception('CUSIP is not length 9')

    def _check_date(self, date, dt_format):
        if isinstance(date, str):
            try:
                datetime.datetime.strptime(date, dt_format)
            except ValueError:
                raise ValueError(
                    'Incorrect data format, should be ' + dt_format)
            return date

        if isinstance(date, datetime.date):
            return date.strftime(dt_format)

    def _check_type(self, s):
        types = ['Bill', 'Note', 'Bond', 'CMB', 'TIPS', 'FRN']
        if s in types:
            return
        else:
            raise ValueError(
                'Incorrect security type format, should be one of (Bill, Note, Bond, CMB, TIPS, FRN)')

    def _process_request(self, url, use_json=True, session=None):

        if session is None:
            r = requests.get(url)
        else:
            r = session.get(url)

        self._raise_status(r)

        if not use_json:
            return io.StringIO(r.text)

        try:
            return r.json()
        except:
            # No data - Bad Issue Date
            return None


class FedSoma(TreasuryFedBase):
    def __init__(self, as_of=None):
        super().__init__()
        self.SOMA_API_BASE_URL = 'http://markets.newyorkfed.org/api/soma/non-mbs/get/ALL/asof/{as_of}.csv'
        self.fed_request_date_fmt = '%Y-%m-%d'

        # Fed SOMA updates are done on Wednesday's, weekday 2 out of the week (0 == monday)
        # if no date passed in, base it off today's date and fetch the latest
        if as_of is None:
            today = datetime.datetime.today()
            if today.weekday() < 2:
                as_of = today - datetime.timedelta(7 - today.weekday())
            elif today.weekday() > 2:
                as_of = today - datetime.timedelta(today.weekday() - 2)
            else:
                as_of = str(as_of).split()[0]

        self.as_of = self._check_date(as_of, self.fed_request_date_fmt)

    def get_soma_cusips(self):
        '''
        Get latest SOMA holdings from the requested date
        Return an interable containing the CUSIPS. 
        It may be a list, set, tuple, etc.

        Optionally, one can return a dict where the keys 
        are the CUSIPS, each one holding another dict with 
        extra metadata about each holding.

        If this is what is returned, TreasuryDirect class 
        method "get_issues_by_cusips" is able to attach
        this SOMA specific metadata to its own data about each CUSIP.

        Keys MUST be the CUSIPS as strings if returning a dict (since looping over a dict
        will be the same as looping over any container, dict just returns .keys() by default when
        looped over)

        {
            '912828S68': {'soma_holding_amount': '1815682000'},
            '912828VQ0': {'soma_holding_amount': '6270000000'},
            '912828QY9': {'soma_holding_amount': '20376532000'},
            '912828K82': {'soma_holding_amount': '482460400'}
        }
        '''
        url = self.SOMA_API_BASE_URL.format(as_of=self.as_of)
        # expecting to get back a file-like object (with .read() and .write())
        # since we disabled json
        soma_request = self._process_request(url, use_json=False)
        if not soma_request or not hasattr(soma_request, 'read'):
            raise RuntimeError(
                'Something went wrong...check url: {} - got back {}'.format(url, soma_request))

        return {
            holding.get('CUSIP'): {'soma_holding_amount': holding.get('Par Value')}
            for holding in csv.DictReader(soma_request)
        }


class TreasuryDirect(TreasuryFedBase):
    def __init__(self):
        super().__init__()
        self.TREASURY_API_BASE_URL = 'https://www.treasurydirect.gov'
        self.SECURITIES_ENDPOINT = '/TA_WS/securities/'
        self.ANNOUNCED_ENDPOINT = '/TA_WS/securities/announced'
        self.AUCTIONED_ENDPOINT = '/TA_WS/securities/announced'
        self.DEBT_ENDPOINT = '/NP_WS/debt/'
        self.SEARCH_ENDPOINT = '/TA_WS/securities/search/'
        self.keep_keys = set([
            'cusip', 'securityType', 'securityTerm', 'issueDate',
            'announcementDate', 'auctionDate', 'bidToCoverRatio',
            'maturingDate', 'offeringAmount', 'originalSecurityTerm',
            'maturityDate'
        ])
        self.date_keys_to_normalize = set([
            'issueDate',
            'announcementDate',
            'auctionDate',
            'maturingDate',
            'maturityDate'
        ])
        self.treasury_date_fmt = '%Y-%m-%dT%H:%M:%S'

    def security_info(self, cusip, issue_date):
        """
        This function returns data about a specific security identified by CUSIP and issue date.
        """
        self._check_cusip(cusip)
        issue_date = self._check_date(issue_date, '%m/%d/%Y')
        url = self.TREASURY_API_BASE_URL + self.SECURITIES_ENDPOINT + \
            '{}/{}?format=json'.format(cusip, issue_date)
        security_dict = self._process_request(url)
        return security_dict

    def security_hist(self, security_type, auction=False, days=7, pagesize=2, reopening='Yes'):
        """
        This function returns data about announced or auctioned securities.  
        Max 250 results.  
        Ordered by announcement date (descending), auction date (descending), issue date (descending), security term length (ascending)
        If auction is true returns auctioned securities
        """
        self._check_type(security_type)
        if auction:
            s = 'auctioned'
        else:
            s = 'announced'
        url = self.TREASURY_API_BASE_URL + self.SECURITIES_ENDPOINT + \
            s + '?format=json' + '&type={}'.format(security_type)
        announced_dict = self._process_request(url)
        return announced_dict

    def security_type(self, security_type):
        """
        This function returns data about securities of a particular type.
        """
        self._check_type(security_type)
        url = self.TREASURY_API_BASE_URL + self.SECURITIES_ENDPOINT + \
            '{}?format=json'.format(security_type)
        security_dict = self._process_request(url)
        return security_dict

    def security_auctions(self, security_type=None, days_ago=250, max_securities=None, reopening=None):
        """
        This function returns data about auctions.
        """

        search_query = {}
        if security_type is not None:
            self._check_type(security_type)
            search_query['type'] = security_type

        if max_securities is not None:
            try:
                search_query['pagesize'] = int(max_securities)
            except:
                raise ValueError(
                    'max_securities needs to be an int or convertible to an int - got {}'.format(type(max_securities)))

        if reopening is not None:
            try:
                search_query['reopening'] = 'Yes' if reopening else 'No'
            except:
                raise ValueError(
                    'reopening needs to be not NoneType, and be convertible to bool - got {}'.format(type(reopening)))

        try:
            search_query['days'] = int(days_ago)
        except:
            raise ValueError(
                'days_ago needs to be an int or convertible to an int - got {}'.format(type(days_ago)))

        # sanity checks passed if we got this far...
        query_url = '&'.join(
            '{key}={value}'.format(key=key, value=value)
            for key, value in search_query.items() if value)

        url = self.TREASURY_API_BASE_URL + \
            self.AUCTIONED_ENDPOINT + '?format=json' + '&' + query_url
        results = self._process_request(url)
        return self._filter_raw_treasury_json(results)

    def security_announcements(self, security_type=None, days_ago=250, max_securities=None, reopening=None):
        """
        This function returns data about auction announcements.
        """

        search_query = {}
        if security_type is not None:
            self._check_type(security_type)
            search_query['type'] = security_type

        if max_securities is not None:
            try:
                search_query['pagesize'] = int(max_securities)
            except:
                raise ValueError(
                    'max_securities needs to be an int or convertible to an int - got {}'.format(type(max_securities)))

        if reopening is not None:
            try:
                search_query['reopening'] = 'Yes' if reopening else 'No'
            except:
                raise ValueError(
                    'reopening needs to be not NoneType, and be convertible to bool - got {}'.format(type(reopening)))

        try:
            search_query['days'] = int(days_ago)
        except:
            raise ValueError(
                'days_ago needs to be an int or convertible to an int - got {}'.format(type(days_ago)))

        # sanity checks passed if we got this far...
        query_url = '&'.join(
            '{key}={value}'.format(key=key, value=value)
            for key, value in search_query.items() if value)

        url = self.TREASURY_API_BASE_URL + \
            self.ANNOUNCED_ENDPOINT + '?format=json' + '&' + query_url
        results = self._process_request(url)
        return self._filter_raw_treasury_json(results)

    def security_search(self, search_query, keep_keys=None, session=None):
        '''
        Search query must be a dict containing some of the following, of particular
        interest is the "dateFieldName", which allows us to point to any other variable
        such as issueDate, maturityDate, calledDate, etc.

        Specifying the dateFieldName gives context to the startDate and endDate parameters, useful
        for narrowing down the search based on a date range for any of the available date fields!
        search_query = dict(
            dateFieldName='issueDate',
            startDate='01/01/2017',
            endDate='01/01/2019',
            type='Bond',
            reopening='Yes'
        )
        search_query = dict(
            cusip='AAB9737A',
        )
        '''
        # date attributes
        start_date, end_date, date_query = search_query.get('startDate', None), search_query.get(
            'endDate', None), search_query.get('dateFieldName', None)
        # type / cusip attributes
        _type, _cusip = search_query.get(
            'type', None), search_query.get('cusip', None)

        # start sanity checks...
        if (start_date is not None or end_date is not None):
            if date_query is None:
                raise ValueError('Please specify a dateFieldName for your query, got {}'.format(
                    repr(search_query)))

            for d in (start_date, end_date):
                if d is not None:
                    self._check_date(d, '%m/%d/%Y')

        if _type is not None:
            self._check_type(_type)

        if _cusip is not None:
            self._check_cusip(_cusip)

        # sanity checks passed if we got this far...
        query_url = '&'.join(
            '{key}={value}'.format(key=key, value=value)
            for key, value in search_query.items() if value)

        url = self.TREASURY_API_BASE_URL + \
            self.SEARCH_ENDPOINT + '?format=json' + '&' + query_url
        results = self._process_request(url, session=session)
        return self._filter_raw_treasury_json(results)

    def _normalize_dates(self, issues):
        # make local scope binds for loop efficiency..too many dots to look into each time
        strptime_, strftime_ = datetime.datetime.strptime, datetime.datetime.strftime
        for issue in issues:
            # go through all dates specified in date_keys_to_normalize, clean them up
            for date_attr in self.date_keys_to_normalize:
                if issue.get(date_attr):
                    date_attr_pyobj = strptime_(
                        issue.get(date_attr), self.treasury_date_fmt)
                    # add a new date attr with _pyobj, so sorting functions can
                    # use objects rather than strings during sorting/grouping.
                    issue[date_attr + '_pyobj'] = date_attr_pyobj
                    # remove the ugly T00:00:00 at the end of the original date_attr
                    issue[date_attr] = strftime_(date_attr_pyobj, '%Y-%m-%d')
            # while still on the same issue, find the proper maturity date. Is it maturing or maturity? lol
            maturing, maturity = issue.get(
                'maturingDate_pyobj', None), issue.get('maturityDate_pyobj', None)
            # if both are present, pick the one out further into future
            if maturing and maturity:
                actual_maturity = maturing if maturing > maturity else maturity
            # otherwise, pick the one out of the two present
            else:
                actual_maturity = maturing or maturity
            # add this new key to each issue to denote the actual maturity
            issue['actual_maturity_pyobj'] = actual_maturity
            issue['actual_maturity'] = strftime_(actual_maturity, '%Y-%m-%d')

        return issues

    def _filter_raw_treasury_json(self, results, keep_keys=None):
        if keep_keys is None:
            keep_keys = set(self.keep_keys)

        return [
            {k: v for k, v in result.items() if k in set(keep_keys)}
            for result in results
        ]

    def _add_extra_data_per_cusip(self, results, enrichment_data=None):

        if enrichment_data is None:
            return results

        for result in results:
            cusip = result['cusip']
            if not enrichment_data.get(cusip):
                continue
            result.update(enrichment_data.get(cusip))

        return results

    def _remove_pyobj_helpers(self, issues):

        for issue in issues:
            pyobj_keys = set([key for key in issue.keys()
                              if key.endswith('_pyobj')])
            for key in pyobj_keys:
                _ = issue.pop(key, None)

        return issues

    def get_maturing_issues_by_date(self, start_date='01/01/2016', end_date='01/01/2019'):
        query_1 = {
            'dateFieldName': 'maturityDate',
            'startDate': start_date,
            'endDate': end_date
        }
        results_1 = self.security_search(query_1)

        query_2 = {
            'dateFieldName': 'maturingDate',
            'startDate': start_date,
            'endDate': end_date
        }
        results_2 = self.security_search(query_2)

        # group them together
        return [] + results_1 + results_2

    def get_issues_by_cusips(self, cusips):
        '''
        Takes an iterable of cusips and passes them through the
        security_search method, in parallel!
        '''
        queries = [
            {'cusip': cusip} for cusip in cusips
        ]

        # some low level stuff one can do to Requests so
        # its unerlying urllib3 uses connection pooling,
        # and we can pass this single session object into
        # a bunch of threads (yes, sharing object...buts its OK!)
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=len(queries),
            pool_maxsize=len(queries),
            max_retries=3)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        futures_holder = []
        with futures.ThreadPoolExecutor(len(queries)) as pool:
            for query in queries:
                futures_holder.append(pool.submit(
                    self.security_search, query, session=session))

        results = []
        for res in futures.as_completed(futures_holder):
            if res.exception():
                print(res.exception())
            else:
                results.extend(res.result())

        # if the <cusips> is a dict, then we can assume its holding enrichment data
        if isinstance(cusips, dict):
            results = self._add_extra_data_per_cusip(
                results, enrichment_data=cusips)

        return results

    def produce_report(self, results, sort_criteria_func=sort_by_week_issued, sort_name='week_issued', soma=False):
        # we will have some duplicates, since both maturityDate and maturingDate
        # vary depending on the type of issue - this just dedupes them
        deduped_normalized_results = self._normalize_dates([
            dict(t) for t in set([tuple(sorted(res.items())) for res in results])
        ])

        issues_by_sort_criteria = itertools.groupby(
            sorted(deduped_normalized_results, key=sort_criteria_func), key=sort_criteria_func
        )

        grouped_data_holder = []

        # these are key names for our nested structure, based on the
        # <sort_name> parameter
        total_agg_timeframe = '{}_total'.format(sort_name)
        total_term_agg_timeframe = '{}_term_total'.format(sort_name)
        issues_agg_timeframe = '{}_issues'.format(sort_name)
        if soma:
            soma_total_agg_timeframe = 'soma_{}_total'.format(sort_name)
            soma_total_term_agg_timeframe = 'soma_{}_term_total'.format(
                sort_name)

        # 1st level of sorting/grouping - based on <sort_criteria_func> callable
        for sort_criteria, issues in issues_by_sort_criteria:

            # since we're using these keys itertools.groupby is returning for
            # our datastructure, its not too friendly for either human consumption
            # nor friendly for Excel - lets convert these into a nice string =)
            try:
                sort_criteria = sort_by_aggregation_normalization(sort_criteria)
            except:
                pass

            # 2nd level of sorting/grouping - based on term of issues
            # NOTE: although we use the keys from itertools.groupby
            # from this 1st grouping in our final datastructure,
            # its statically defined, and a string already, already
            # good enough for consumption - ie '10-Year', or '7-Year'
            issues_by_criteria_by_term = itertools.groupby(
                sorted(issues, key=sort_by_term), key=sort_by_term)

            grouped_data = {}
            grouped_data['timeframe'] = sort_criteria
            grouped_data[total_agg_timeframe] = 0
            if soma:
                # if looking at SOMA specific issues, we need to keep a separate
                # counter
                grouped_data[soma_total_agg_timeframe] = 0

            grouped_data[issues_agg_timeframe] = {}

            for term, term_issues in issues_by_criteria_by_term:
                grouped_data[issues_agg_timeframe][term] = {}
                term_isues_date = list(
                    sorted(term_issues, key=sort_criteria_func))
                total_per_criteria_per_term = sum(
                    int(i['offeringAmount']) for i in term_isues_date)
                grouped_data[issues_agg_timeframe][term][total_term_agg_timeframe] = total_per_criteria_per_term
                grouped_data[total_agg_timeframe] += total_per_criteria_per_term

                # special handling for soma securities, which have key 'soma_holding_amount' to agg on
                if soma:
                    soma_total_per_criteria_per_term = sum(
                        int(i['soma_holding_amount']) for i in term_isues_date)
                    grouped_data[issues_agg_timeframe][term][soma_total_term_agg_timeframe] = soma_total_per_criteria_per_term
                    grouped_data[soma_total_agg_timeframe] += soma_total_per_criteria_per_term

                # once done with processing, remove all the _pyobj helpers =)
                grouped_data[issues_agg_timeframe][term]['issues'] = self._remove_pyobj_helpers(
                    term_isues_date)

            grouped_data_holder.append(grouped_data)

        return grouped_data_holder

    def current_debt(self):
        """
        This function returns the most recent debt data.
        """
        url = self.TREASURY_API_BASE_URL + self.DEBT_ENDPOINT + 'current?format=json'
        debt = self._process_request(url)
        return debt

    def get_debt_by_date(self, dt):
        """
        This function returns the debt data for a particular date.
        """
        dt = self._check_date(dt, '%Y/%m/%d')
        url = self.TREASURY_API_BASE_URL + \
            self.DEBT_ENDPOINT + '{}?format=json'.format(dt)
        debt = self._process_request(url)
        return debt

    def get_debt_range(self, start_dt, end_dt):
        """
        This function returns debt data based on the parameters passed.  
        """
        start_dt = self._check_date(start_dt, '%Y-%m-%d')
        end_dt = self._check_date(end_dt, '%Y-%m-%d')
        url = self.TREASURY_API_BASE_URL + self.DEBT_ENDPOINT + \
            'search?startdate={}&enddate={}&format=json'.format(
                start_dt, end_dt)
        debt = self._process_request(url)
        return debt

def get_reports_for_all():
    '''
    test out the lib. Import it and call this function
    it should take a few seconds, adjust as needed using how much
    data we query for via the <days_ago> and <start|end_date> arg
    passed to get_maturing_issues_by_date method of TreasuryDirect

    >>> import treasury_direct
    >>> all_reports = treasury_direct.get_reports_for_all()
    >>> all_reports.keys()
        dict_keys(['soma_holdings', 'announced', 'auctioned', 'maturing'])

    >>> # each one of these keys holds all the data about its corresponding category
    >>> # they all have a list of dicts, each dict the same keys:

    >>> all_reports['auctioned'][0].keys()
        dict_keys(['timeframe', 'month_auctioned_total', 'month_auctioned_issues'])

    >>> # soma_holding entries have one extra key, soma_month_maturing_total
    >>> all_reports['soma_holdings'][0].keys()
        dict_keys(['timeframe', 'month_maturing_total', 'soma_month_maturing_total', 'month_maturing_issues'])

    >>> # sample for interacting with the data -> get total $ for all issues
    >>> # maturing on a monthly aggregation timeframe

    >>> [(issues['month_maturing_total'], issues['timeframe']) for issues in all_reports['maturing']]
        [(631000000000, '2016-1'),
        (581000000000, '2016-2'),
        (692000000000, '2016-3'),
        (663000000000, '2016-4'),
        (584000000000, '2016-5'),
        (670000000000, '2016-6'),
        (610000000000, '2016-7'),
    >>> 
    '''

    treasury = TreasuryDirect()
    soma = FedSoma()
    data = {
        'soma_holdings': treasury.produce_report(
            treasury.get_issues_by_cusips(soma.get_soma_cusips()),
            sort_criteria_func=sort_by_month_maturity, sort_name='month_maturing', soma=True),
        'announced': treasury.produce_report(
            treasury.security_announcements(days_ago=365),
            sort_criteria_func=sort_by_month_announced, sort_name='month_announced'),
        'auctioned': treasury.produce_report(
            treasury.security_auctions(days_ago=365),
            sort_criteria_func=sort_by_month_auctioned, sort_name='month_auctioned'),
        'maturing': treasury.produce_report(
            treasury.get_maturing_issues_by_date(
                start_date='01/01/2016', end_date='01/01/2021'),
            sort_criteria_func=sort_by_month_maturity, sort_name='month_maturing')
    }
    return data
