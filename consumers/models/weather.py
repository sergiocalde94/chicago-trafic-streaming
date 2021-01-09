"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        try:
            value = message.value()
            self.temperature = value.temperature
            self.status = value.status
        except Exception as e:
            logger.error(f"weather process_message is incomplete: {e} - skipping")
