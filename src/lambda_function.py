import logging

from events import handle_event


# noinspection PyUnusedLocal
def lambda_handler(event, context):
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Code version: $VERSION$")
    try:
        result = handle_event(event)
        logging.info(result)
        return result
    except Exception as e:
        logging.error(f"Error running lambda_handler: {str(e)}")
        return {"error": str(e)}
