import json
import logging
from datetime import datetime
from typing import Final, Literal

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
COLORS = {
    "DEBUG": "#00BCD4",
    "INFO": "#2196F3",
    "WARNING": "#FF9800",
    "ERROR": "#F44336",
    "CRITICAL": "#C62828",
}

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

    if _now.month == 10 and _now.day == 31:
        DEFAULT_LOGGER.info("🎃👻 Happy Halloween! May your data be spooky good and your bugs be few! 🕸️🦇")

    if _now.month == 7 and _now.day == 4:
        DEFAULT_LOGGER.info("🎆🇺🇸 Happy 4th of July! Celebrate freedom with flawless data processing! 🎇🍔")

    if _now.month == 1 and _now.day == 1:
        DEFAULT_LOGGER.info("🎉 Happy New Year! Wishing you a year of successful data projects and clean code! 🥳🎆")

    if _now.month == 2 and _now.day == 14:
        DEFAULT_LOGGER.info(
            "💖 Happy Valentine's Day! May your data relationships be strong and your joins be perfect! 🌹📊"
        )

    if _now.month == 3 and _now.day == 17:
        DEFAULT_LOGGER.info(
            "🍀 Happy St. Patrick's Day! Wishing you the luck of the Irish in all your data endeavors! 🌈☘️"
        )

    if _now.month == 6 and _now.day == 19:
        DEFAULT_LOGGER.info(
            "🌞 Happy Juneteenth! Celebrating freedom and the power of data to enlighten and empower! ✊🏿📈"
        )

    if _now.month == 11 and _now.day == 11:
        DEFAULT_LOGGER.info(
            "🦃 Happy Veterans Day! Honoring those who served while we serve up great data insights! 🇺🇸📊"
        )

    if _now.month == 5 and _now.day == 1:
        DEFAULT_LOGGER.info("🌸 Happy May Day! Celebrating spring and the blossoming of new data opportunities! 🌷📈")

    if _now.month == 5 and _now.day == 5:
        DEFAULT_LOGGER.info("🎉 Happy Cinco de Mayo! Celebrating culture and the fiesta of data analytics! 🌮📊")

    if _now.month == 5 and _now.day == 4:
        DEFAULT_LOGGER.info(
            "🌌 May the 4th be with you! Harness the force of data to conquer your analytics challenges! 🚀📊"
        )

    if _now.month == 8 and _now.day == 1:
        DEFAULT_LOGGER.info(
            "🏖️ Happy Swiss National Day! Celebrating precision and excellence in data, just like Swiss craftsmanship! 🇨🇭📈"
        )

    if _now.month == 7 and _now.day == 14:
        DEFAULT_LOGGER.info(
            "🎉 Happy Bastille Day! Celebrating liberty, equality, and the power of data to transform societies! 🇫🇷📊"
        )

    if _now.month == 7 and _now.day == 21:
        DEFAULT_LOGGER.info(
            "🚴 Happy Belgian National Day! Celebrating unity and the strength of data-driven decisions! 🇧🇪📈"
        )

    if _now.day > 27 and _now.day < 31:
        DEFAULT_LOGGER.warning(
            "⚠️ Warning: End of month is near! Make sure to finalize your data reports and close out any pending tasks! 📅✅"
        )

    if _now.weekday() == 4:
        DEFAULT_LOGGER.warning(
            "📅🚫 Please do not deploy on Fridays! Avoid end-of-week surprises in your data pipelines! 🚫📅"
        )

    if _now.weekday() == 0:
        DEFAULT_LOGGER.info(
            "☕ Happy Monday! Kickstart your week with fresh data insights and a strong cup of coffee! 📊☕"
        )


def send_message_to_channel(
    channel: str,
    title: str,
    message: str,
    color: str | None = None,
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
        color = COLORS[loglevel]
        teams_message["themeColor"] = color

    teams_message["text"] = message

    teams_message_json = json.dumps(teams_message)

    response = requests.post(webhook_url, data=teams_message_json, headers={"Content-Type": "application/json"})
    if response.status_code == 200:
        return True
    else:
        return False
