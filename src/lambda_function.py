import logging

from events import handle_event


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Code version: $VERSION$")
    result = handle_event(event)
    logging.info(result)
    return result
