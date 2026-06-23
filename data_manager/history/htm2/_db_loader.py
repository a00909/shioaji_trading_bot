from data_manager.history.base2._db_loader_base import _DbLoaderBase
from data_manager.history.statics.tick._tick_spec import tick_spec
from data_manager.history.statics.tick.np_ticks import NPTicks


class DBLoader(_DbLoaderBase[NPTicks]):
    def __init__(self):
        super().__init__( tick_spec)
