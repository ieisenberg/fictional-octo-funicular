import os
import gnupg
from typing import Optional, List, Dict, Any

from src.error_handling import EncryptionError, logger

class Encryptor:
    def __init__(self, gpg_home: Optional[str] = None, recipient_key: Optional[str] = None):
        self.gpg_home = gpg_home
        
        self.recipient_key = recipient_key or os.environ.get('GPG_RECIPIENT_KEY')
        if not self.recipient_key:
            logger.warning("No GPG recipient key provided. Using default key.")
        
        try:
            self.gpg = gnupg.GPG(gnupghome=self.gpg_home)
        except Exception as e:
            raise EncryptionError(f"Failed to initialize GPG: {str(e)}")
    
    def encrypt_string(self, data: str) -> str:
        try:
            encrypted_data = self.gpg.encrypt(data, recipients=[self.recipient_key], always_trust=True)
            
            if not encrypted_data.ok:
                raise EncryptionError(f"Encryption failed: {encrypted_data.status}")
            
            return str(encrypted_data)
        except Exception as e:
            raise EncryptionError(f"Error during encryption: {str(e)}")
    
    def encrypt_file(self, input_path: str, output_path: str) -> str:
        try:
            with open(input_path, 'rb') as f:
                encrypted_data = self.gpg.encrypt_file(f, recipients=[self.recipient_key], always_trust=True, output=output_path)
            
            if not encrypted_data.ok:
                raise EncryptionError(f"File encryption failed: {encrypted_data.status}")
            
            return output_path
        except Exception as e:
            raise EncryptionError(f"Error during file encryption: {str(e)}")
    
    def encrypt_events(self, events: List[Dict[Any, Any]], output_path: str) -> str:
        try:
            from src.processing import events_to_jsonl
            jsonl_content = events_to_jsonl(events)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            encrypted_data = self.encrypt_string(jsonl_content)
            with open(output_path, 'w') as f:
                f.write(encrypted_data)
            
            return output_path
        except Exception as e:
            raise EncryptionError(f"Error encrypting events: {str(e)}")

def encrypt_data(events: List[Dict[Any, Any]], filename: str, base_dir: str = "data") -> str:
    encryptor = Encryptor()
    output_path = os.path.join(base_dir, f"{filename}.gpg")
    return encryptor.encrypt_events(events, output_path)