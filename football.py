import requests
import pandas as pd
from datetime import datetime

class FootBall(requests.Session):
	def _logException(*outer_args):
		def check(*args, **kwargs):
			try: 
				return outer_args[-1](*args, **kwargs)
			except Exception as e:
				print('[-] ERROR:', e)
				raise RuntimeError
		return check

	@_logException
	def __init__(self, debug_level='ERROR'):
		super().__init__()
		self._debug_level = debug_level

		self.DEBUG_LEVEL = dict([i[::-1] for i in enumerate(['INFO', 'SUCCESS', 'WARNING', 'ERROR'])])
		self.MARKET_URL = 'https://rgs.betradar.com/bgw-services-af-rest/rest/bookmakers/{}/markets?key=pK9saJZcyZRVRgZ9&ptype=vfl&event={}&lang=en'
		self.TEAM_LIST_URL = 'https://rgs.betradar.com/bgw-services-af-rest/rest/bookmakers/27/events?ptype=vfl&key={}&tag=vfl-{}-{}&lang=en'
		self.META_URL = 'https://rgs.betradar.com/vflkcgaming/timeline.php?lang=en&screen=vleague'
		self.API_KEY_URL = 'https://virtual.bet9ja.com/betradardesktopmenu/IntegrationBetradar/getGames'

		self._debug('SUCCESS', 'Session initialized')
		self.api_key = self._get_api_key()
		self._debug('SUCCESS', 'Parsed API key')
		self._debug('INFO', f'Current API key: {self.api_key}')
		self.season_id, self.match_day = self._get_season_meta()
		self._debug('SUCCESS', 'Season metadata parsed successfully')
		self._debug('INFO', f'Current season ID: {self.season_id}')
		self._debug('INFO', f'Current matchday: {self.match_day}')


	@_logException
	def _get_season_meta(self):
		self._debug('INFO', 'Requesting season metadata')
		raw = self._make_request(self.META_URL).json()
		return raw["season_name"].split()[-1], raw["matchday"]

	@_logException
	def _get_api_key(self):
		self._debug('INFO', 'Requesting API key')
		raw = self._make_request(self.API_KEY_URL).text
		start = raw.find('key=')
		end = raw.find('&', start)
		return raw[start+4:end]

	@_logException
	def _make_request(self, url, headers=None):
		while True:
			try: return self.get(url, headers=headers, timeout=5)
			except: self._debug('WARNING', 'Request failed. Retrying')

	@_logException
	def _get_team_list(self):
		self._debug('INFO', 'Getting team list')
		return [{
			'url': self.MARKET_URL.format(i['bookmakerId'], i['uniformId']),
			'competitors': ' - '.join(j["teamName"] for j in i['competitors'])
		} for i in self._make_request(self.TEAM_LIST_URL.format(self.api_key, self.season_id, self.match_day)).json()['data'][0]['events']]

	@_logException
	def get_full(self, output='csv'):
		results = self._get_team_list()
		self._debug('SUCCESS', 'team list parsed successfully')
		self._debug('INFO', 'Requesting market data for each competitors')
		for i in range(len(results)):
			self._debug('INFO', 'Requesting data for {}'.format(results[i]['competitors']))
			for j in self._make_request(results[i]['url']).json()["data"][0]["markets"]:
				if j['market'][0]["sortIndex"] == 1:
					results[i]['time'] = datetime.fromtimestamp(j['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
					for k in j['market'][0]['selections']:
						results[i][k['description']] = float(k['odds'])
			self._debug('SUCCESS', 'Data for {} requested successfully'.format(results[i]['competitors']))
		if output in ['json', 'csv']:
			self._debug('SUCCESS', 'Parsing success. Returning result in {} format'.format(output))
			if output == 'csv': return pd.DataFrame(results)
			elif output == 'json': return results
		else:
			raise TypeError('Please provide a valid output format')

	@_logException
	def _debug(self, level ,text):
		if self.DEBUG_LEVEL[level] >= self.DEBUG_LEVEL[self._debug_level]:
			if level == 'ERROR':
				print('[-] ERROR:', text)
			elif level == 'WARNING':
				print('[!] WARNING:', text)
			elif level == 'INFO':
				print('[*] INFO:', text)
			elif level == 'SUCCESS':
				print('[+] SUCCESS:', text)

if __name__ == '__main__':
	FootBall(debug_level='INFO').get_full().to_csv('football.csv')