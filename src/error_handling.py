import logging
import time
import sys
from functools import wraps
from typing import Callable, TypeVar, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("log.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("build")

T = TypeVar('T')

def with_exponential_backoff(
    max_retries: int = 5,
    initial_wait: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,)
) -> Callable:
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            retries = 0
            wait_time = initial_wait
            
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded for {func.__name__}: {str(e)}")
                        raise
                    
                    logger.warning(
                        f"Retry {retries}/{max_retries} for {func.__name__} after error: {str(e)}. "
                        f"Waiting {wait_time:.2f}s"
                    )
                    time.sleep(wait_time)
                    wait_time *= backoff_factor
        
        return wrapper
    
    return decorator

class ProcessingError(Exception):
    pass

class DownloadError(ProcessingError):
    pass

class EncryptionError(ProcessingError):
    pass

class GitOperationError(ProcessingError):
    pass

def log_error(error: Exception, context: str = "") -> None:
    if context:
        logger.error(f"{context}: {str(error)}")
    else:
        logger.error(str(error))

def setup_exception_handler() -> None:
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
            
        logger.critical("Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback))
        
    sys.excepthook = handle_exception