import os
import sys

from src.acquisition import stream_events, get_date_range
from src.processing import process_hourly_data
from src.encryption import encrypt_data
from src.storage import MetadataManager, GitManager, ensure_directory_exists
from src.error_handling import logger, setup_exception_handler, ProcessingError

DATA_DIR = "data"
MID = int(os.environ.get("MID"))
START_YEAR = 2020
START_MONTH = 1
START_DAY = 1

if not os.environ.get("MID"):
    logger.error("MID environment variable not set.")
    sys.exit(1)

metadata_manager = MetadataManager()

def process_day(year: int, month: int, day: int) -> bool:

    logger.info(f"Processing day: {year}-{month:02d}-{day:02d}")
    
    all_events = []

    git_manager = GitManager()
    
    try:
        for hour in range(24):
            events_generator = stream_events(year, month, day, hour)
            hourly_events = process_hourly_data(events_generator, MID)
            
            all_events.extend(hourly_events)
        
        if all_events:            
            ensure_directory_exists(DATA_DIR)
            
            filename = f"{year}-{month:02d}-{day:02d}.jsonl"
            output_path = encrypt_data(all_events, filename, DATA_DIR)
            
            metadata_manager.mark_day_processed(year, month, day)
            
            git_manager.commit_and_push(
                f"Add events for {year}-{month:02d}-{day:02d}",
                [output_path, metadata_manager.metadata_path]
            )
            
            return True
        else:
            metadata_manager.mark_day_processed(year, month, day)
            
            git_manager.commit_and_push(
                f"Mark {year}-{month:02d}-{day:02d} as processed",
                [metadata_manager.metadata_path]
            )
            
            return False
    except Exception as e:
        logger.error(f"Error processing day {year}-{month:02d}-{day:02d}: {str(e)}")
        raise ProcessingError(f"Error processing day {year}-{month:02d}-{day:02d}: {str(e)}")


def main() -> None:
    setup_exception_handler()
        
    try:
        all_days = list(get_date_range(START_YEAR, START_MONTH, START_DAY))
        
        days_to_process = metadata_manager.get_unprocessed_days(all_days)
        
        if not days_to_process:
            return
        
        for year, month, day in days_to_process:
            try:
                process_day(year, month, day)
            except ProcessingError as e:
                logger.error(f"Failed to process day {year}-{month:02d}-{day:02d}: {str(e)}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing day {year}-{month:02d}-{day:02d}: {str(e)}")
                continue
        
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()