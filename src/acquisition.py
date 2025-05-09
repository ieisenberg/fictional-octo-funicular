import requests
import gzip
import json
from datetime import datetime, timedelta
from typing import Generator, Dict, Any, Optional, Tuple

from src.error_handling import with_exponential_backoff, DownloadError, logger

GITHUB_ARCHIVE_URL = "https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"

@with_exponential_backoff(max_retries=5, exceptions=(requests.RequestException,))
def download_hourly_data(year: int, month: int, day: int, hour: int) -> requests.Response:
    url = GITHUB_ARCHIVE_URL.format(year=year, month=month, day=day, hour=hour)
    
    response = requests.get(url, stream=True)
    
    if response.status_code != 200:
        raise DownloadError(f"Failed to download {url}, status code: {response.status_code}")
    
    return response

def stream_events(year: int, month: int, day: int, hour: int) -> Generator[Dict[Any, Any], None, None]:
    try:
        response = download_hourly_data(year, month, day, hour)
        
        with gzip.GzipFile(fileobj=response.raw) as gz_file:
            for line in gz_file:
                try:
                    event = json.loads(line.decode('utf-8'))
                    yield event
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON: {str(e)}")
                    continue
                except Exception as e:
                    logger.warning(f"Error processing event: {str(e)}")
                    continue
    except Exception as e:
        raise DownloadError(f"Error streaming events for {year}-{month:02d}-{day:02d}-{hour}: {str(e)}")

def generate_date_range(start_date: datetime, end_date: datetime) -> Generator[Tuple[int, int, int], None, None]:
    current_date = start_date
    while current_date <= end_date:
        yield (current_date.year, current_date.month, current_date.day)
        current_date += timedelta(days=1)

def is_valid_date(year: int, month: int, day: int) -> bool:
    try:
        datetime(year, month, day)
        return True
    except ValueError:
        return False

def get_date_range(start_year: int = 2020, start_month: int = 1, start_day: int = 1,
                  end_date: Optional[datetime] = None) -> Generator[Tuple[int, int, int], None, None]:
    if not is_valid_date(start_year, start_month, start_day):
        raise ValueError(f"Invalid start date: {start_year}-{start_month}-{start_day}")
    
    start_date = datetime(start_year, start_month, start_day)
    if end_date is None:
        end_date = datetime.now()
    
    return generate_date_range(start_date, end_date)