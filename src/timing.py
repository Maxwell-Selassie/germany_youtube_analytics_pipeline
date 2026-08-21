import time
from contextlib import contextmanager

@contextmanager
def timer(name, logger):
    start = time.perf_counter()
    
    logger.info("Starting: {name}")
    
    try:
        yield
        
    finally:
        elapsed = time.perf_counter() - start
        logger.info(
            f"Finished: {name} duration={elapsed:.2f}s"
        )