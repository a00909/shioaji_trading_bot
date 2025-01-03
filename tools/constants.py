from datetime import timezone, timedelta

from shioaji import shioaji

DATE_FORMAT_SHIOAJI = '%Y-%m-%d'
DATE_FORMAT_REDIS = '%Y.%m.%d'
DEFAULT_TIMEZONE = timezone(timedelta(hours=8))
UTC_TZ = timezone(timedelta(hours=0))