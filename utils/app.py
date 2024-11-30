import os
import time

from dotenv import load_dotenv
from redis.client import Redis
from shioaji import Shioaji
from shioaji.contracts import FetchStatus
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from utils import RedisManager
from tick_manager.history_tick_manager import HistoryTickManager

import shioaji as sj


class App:
    def __init__(self, init=False, simu=True):
        self.redis: Redis = None
        self.history_tick_manager: HistoryTickManager = None
        self.api: Shioaji = None
        self.session_maker: sessionmaker[Session] = None
        self.engine = None
        self.simu = simu
        self.contract = None
        if init:
            self.init()

    def init(self):
        load_dotenv()
        self.redis = RedisManager().redis
        self.login_api()
        self.engine = create_engine(os.getenv('DB_URL'))
        self.session_maker = sessionmaker(bind=self.engine, expire_on_commit=False)

        self.history_tick_manager = HistoryTickManager(self.api, self.redis, self.session_maker)
        self.contract = self.api.Contracts.Futures.TMF.TMFR1

    def set_contract(self, contract):
        self.contract = contract

    def get_contract(self):
        return self.contract

    def shut(self):
        self.api.logout()

    def login_api(self):
        if not self.api:
            self.api = sj.Shioaji(simulation=self.simu)
        else:
            self.api.logout()

        self.api.login(
            api_key=os.environ["API_KEY"],
            secret_key=os.environ["SECRET_KEY"],
        )
        self.api.activate_ca(
            ca_path=os.environ["CA_CERT_PATH"],
            ca_passwd=os.environ["CA_PASSWORD"],
        )
        while self.api.Contracts.status == FetchStatus.Fetching:
            print(f'Contracts status: {self.api.Contracts.status}')
            time.sleep(2)
