import json
import logging
from datetime import datetime
from typing import Final, Literal, Optional

import requests

from fabricks.context import IS_DEBUGMODE, IS_FUNMODE, LOGLEVEL, SECRET_SCOPE, TIMEZONE
from fabricks.utils.log import get_logger

logger, _ = get_logger(
    "logs",
    LOGLEVEL,
    table=None,
    debugmode=IS_DEBUGMODE,
    timezone=TIMEZONE,
)
logging.getLogger("SQLQueryContextLogger").setLevel(logging.CRITICAL)

DEFAULT_LOGGER: Final[logging.Logger] = logger

if IS_FUNMODE:
    # 🎄 Christmas Easter Egg 🎅
    _now = datetime.now()
    if _now.month == 12:
        _day = _now.day
        if _day <= 24:
            _days_until = 25 - _day
            if _days_until == 1:
                DEFAULT_LOGGER.info("🎄 Ho ho ho! Only 1 day until Christmas! Happy data processing! 🎅")
            elif _days_until <= 7:
                DEFAULT_LOGGER.info(
                    f"🎄 'Tis the season! {_days_until} days until Christmas! May your pipelines run smoothly! 🎁"
                )
            else:
                DEFAULT_LOGGER.info("🎄 Merry December! Wishing you bug-free data pipelines this holiday season! ⛄")
        elif _day == 25:
            DEFAULT_LOGGER.info("🎄🎅 MERRY CHRISTMAS! May all your queries be optimized and your data be clean! 🎁✨")
        else:
            DEFAULT_LOGGER.info("🎄 Happy Holidays! Hope you're enjoying the festive season between data runs! 🎉")


def send_message_to_channel(
    channel: str,
    title: str,
    message: str,
    color: Optional[str] = None,
    loglevel: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO",
) -> bool:
    """
    Send a message to Microsoft Teams via webhook

    Args:
        webhook_url (str): The webhook URL for your Teams channel
        message (str): The message to send
        title (str, optional): Title for the message card
        color (str, optional): Hex color for the message card)

    Returns:
        bool: True if message was sent successfully, False otherwise
    """
    from databricks.sdk.runtime import dbutils

    channel = channel.lower()
    channel = channel.replace(" ", "-")
    webhook_url = dbutils.secrets.get(scope=SECRET_SCOPE, key=f"{channel}-webhook-url")

    teams_message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": title,
    }

    if title:
        teams_message["title"] = title

    if color:
        teams_message["themeColor"] = color
    else:
        COLORS = {
            "DEBUG": "#00FFFF",
            "INFO": "#00FF00 ",
            "WARNING": "#FFFF00 ",
            "ERROR": "#FF0000 ",
            "CRITICAL": "#FF0000",
        }
        color = COLORS[loglevel]
        teams_message["themeColor"] = color

    teams_message["text"] = message

    teams_message_json = json.dumps(teams_message)

    response = requests.post(webhook_url, data=teams_message_json, headers={"Content-Type": "application/json"})
    if response.status_code == 200:
        return True
    else:
        return False
