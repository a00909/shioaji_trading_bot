import atexit
import os
import threading

import shioaji as sj
from dotenv import load_dotenv
from redis.client import Redis
from shioaji import Shioaji
from shioaji.constant import SecurityType
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from data_manager.history_data_manager.history_tick_manager import HistoryTickManager
from tools.logger.custom_logger import CustomLogger
from tools.redis_manager import RedisManager


class App:
    def __init__(self, simu=True):
        self._redis: Redis = None
        self._history_tick_manager: HistoryTickManager = None
        self._api: Shioaji = None
        self._session_maker: sessionmaker[Session] = None
        self._engine = None
        self._simu = simu
        self._api_started = False
        self._contract_waiting = {SecurityType.Index, SecurityType.Future, SecurityType.Stock, SecurityType.Option}

        self._logger = CustomLogger.get_logger('app')

    def shut(self):
        if self._api_started:
            self._logger.info(self._api.usage())
            self._api.logout()
            self._api_started = False
            self._logger.info(f"shioaji logged out.")

    def _login_api(self):
        if self._api:
            return

        self._api = sj.Shioaji(simulation=self._simu)
        fetched_event = threading.Event()
        fetched_contracts = set()

        def api_contract_cb(security_type: SecurityType = None):
            self._logger.info(f"{repr(security_type)} fetch done. overall status: {self._api.Contracts.status}.")
            fetched_contracts.add(security_type)
            if fetched_contracts.issuperset(self._contract_waiting):
                fetched_event.set()

        self._api.login(
            api_key=os.environ["API_KEY"],
            secret_key=os.environ["SECRET_KEY"],
            contracts_cb=api_contract_cb
        )
        self._api.activate_ca(
            ca_path=os.environ["CA_CERT_PATH"],
            ca_passwd=os.environ["CA_PASSWORD"],
        )
        fetched_event.wait()
        self._api_started = True
        atexit.register(self.shut)

    def get_default_account(self):
        return self._api.futopt_account

    @property
    def api(self):
        if not self._api:
            self._login_api()
        return self._api

    @property
    def redis(self):
        if not self._redis:
            self._redis = RedisManager().redis
        return self._redis

    @property
    def engine(self):
        if not self._engine:
            load_dotenv()
            self._engine = create_engine(os.getenv('DB_URL'))
        return self._engine

    @property
    def session_maker(self):
        if not self._session_maker:
            self._session_maker = sessionmaker(bind=self.engine, expire_on_commit=False)
        return self._session_maker

    @property
    def history_tick_manager(self):
        if not self._history_tick_manager:
            self._history_tick_manager = HistoryTickManager(self.api, self.redis, self.session_maker)
        return self._history_tick_manager
