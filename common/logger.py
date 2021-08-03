import logging
from datetime import datetime


logger = None


def setup(name = None):

    global logger

    if name is None:
        name = __name__

    # create file handler which logs even debug messages
    fh = logging.FileHandler('%s_%s-ocr_poc_logger.log' % (name, str(datetime.now())))
    fh.setLevel(logging.DEBUG)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    fhFormatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    chFormatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - Line: %(lineno)d - %(message)s')
    fh.setFormatter(fhFormatter)
    ch.setFormatter(chFormatter)

    # add the handlers to logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    logger.addHandler(ch)
    logger.addHandler(fh)

