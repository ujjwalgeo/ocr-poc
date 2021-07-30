import logging


logger = None


def setup():

    global logger

    # create file handler which logs even debug messages
    fh = logging.FileHandler('ocr_poc_logger.log')
    fh.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    fhFormatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    chFormatter = logging.Formatter('%(levelname)s - %(filename)s - Line: %(lineno)d - %(message)s')
    fh.setFormatter(fhFormatter)
    ch.setFormatter(chFormatter)

    # add the handlers to logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    logger.addHandler(ch)
    logger.addHandler(fh)

