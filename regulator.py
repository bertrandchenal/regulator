import argparse
from random import random
from time import sleep
import socket

import consul
import requests

LEADER_KEY = 'service/regulator/leader'


def hostname():
    return socket.gethostname()

def rsleep(seconds=1):
    'Randomized sleep'
    sleep(seconds + random() * seconds)

class Connection:

    def __init__(self, name=None):
        self.consul = consul.Consul()
        self.name = name
        self.session = None

    def election(self):
        '''
        Try to become leader
        '''
        session = self.consul.session.create(ttl=10)
        while True:
            # Blocking wait if a leader is present
            index, data = self.consul.kv.get(LEADER_KEY)
            while data and data.get('Session'):
                print('waiting for changes')
                index, data = self.consul.kv.get(LEADER_KEY, index=index)

            success = self.consul.kv.put(LEADER_KEY, self.name, acquire=session) # TODO session may be already stalled!
            if success:
                self.session = session
                return True
            else:
                print(':-(')
                rsleep()

    def monitor(self):
        '''
        Send commands when a service become unreachable
        '''

        # Here be dragons
        print('%s was elected' % self.name)
        rsleep()

    def cleanup(self):
        print('CLEANUP!')
        if self.session:
            try:
                self.consul.kv.put(LEADER_KEY, self.name, release=self.session)
                self.consul.session.destroy(self.session)
            except consul.base.ConsulException as e :
                # Consul may return an error if the session has become invalid
                print(e)
            finally:
                self.session = None

    def main(self):
        while True:
            try:
                success = self.election()
                print('SUCESS')
                # When election returns we are leader
                self.monitor()
                # Release active session
                self.cleanup()
            except requests.exceptions.RequestException:
                # Connection issue, we wait a bit and we try again
                rsleep()
                pass
            except KeyboardInterrupt:
                self.cleanup()
                return

def cli():
    default_name =  hostname()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-n', '--name', default=default_name,
        help='Choose node name (defaults to %s)' % default_name,
    )
    return parser.parse_args()

if __name__ == '__main__':
    args = cli()
    connection = Connection(**vars(args))
    connection.main()
