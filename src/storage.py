import os
import json
from typing import Dict, Any, List, Tuple
from datetime import datetime
import git

from src.error_handling import GitOperationError, logger, with_exponential_backoff

class MetadataManager:
    def __init__(self, metadata_path: str = "metadata.json"):
        self.metadata_path = metadata_path
        self.metadata = self._load_metadata()
    
    def _load_metadata(self) -> Dict[str, Any]:
        if os.path.exists(self.metadata_path):
            try:
                with open(self.metadata_path, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse metadata file {self.metadata_path}, creating new metadata")
                return self._create_default_metadata()
            except Exception as e:
                logger.warning(f"Error loading metadata: {str(e)}, creating new metadata")
                return self._create_default_metadata()
        else:
            return self._create_default_metadata()
    
    def _create_default_metadata(self) -> Dict[str, Any]:
        return {
            "last_updated": datetime.now().isoformat(),
            "processed_days": [],
            "version": "1.0.0"
        }
    
    def save_metadata(self) -> None:
        self.metadata["last_updated"] = datetime.now().isoformat()
        
        try:
            with open(self.metadata_path, 'w') as f:
                json.dump(self.metadata, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save metadata: {str(e)}")
    
    def mark_day_processed(self, year: int, month: int, day: int) -> None:
        day_key = f"{year}-{month:02d}-{day:02d}"
        
        if "processed_days" not in self.metadata:
            self.metadata["processed_days"] = []
        
        if day_key not in self.metadata["processed_days"]:
            self.metadata["processed_days"].append(day_key)
        
        self.save_metadata()
    
    def is_day_processed(self, year: int, month: int, day: int) -> bool:
        day_key = f"{year}-{month:02d}-{day:02d}"
        
        if "processed_days" not in self.metadata:
            return False
        
        return day_key in self.metadata["processed_days"]
    
    def get_unprocessed_days(self, days: List[Tuple[int, int, int]]) -> List[Tuple[int, int, int]]:
        return [(year, month, day) for year, month, day in days 
                if not self.is_day_processed(year, month, day)]


class GitManager:
    def __init__(self, repo_path: str = "."):
        try:
            self.repo = git.Repo(repo_path)
        except git.exc.InvalidGitRepositoryError:
            raise GitOperationError(f"{repo_path} is not a valid Git repository")
        except Exception as e:
            logger.error(f"Error initializing Git repository: {str(e)}")
            raise GitOperationError(f"Error initializing Git repository: {str(e)}")
    
    @with_exponential_backoff(max_retries=3, exceptions=(git.exc.GitCommandError,))
    def commit_and_push(self, message: str, paths: List[str]) -> None:
        try:
            for path in paths:
                self.repo.git.add(path)
            
            self.repo.git.commit('-m', message)
            
            self.repo.git.push('origin', self.repo.active_branch.name)
        except git.exc.GitCommandError as e:
            logger.error(f"Git operation failed: {str(e)}")
            raise GitOperationError(f"Git operation failed: {str(e)}")
        except Exception as e:
            logger.error(f"Error during Git operation: {str(e)}")
            raise GitOperationError(f"Error during Git operation: {str(e)}")

def ensure_directory_exists(directory: str) -> None:
    os.makedirs(directory, exist_ok=True)
    logger.debug(f"Ensured directory exists: {directory}")