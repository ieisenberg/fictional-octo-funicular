from typing import Dict, Any, List, Generator
import json

def filter_events_by_user(events: Generator[Dict[Any, Any], None, None], 
                         md_id: int) -> List[Dict[Any, Any]]:
    filtered_events = []
    event_count = 0
    matched_count = 0
    
    for event in events:
        event_count += 1
        
        is_match = False
        
        if 'actor' in event and event['actor'] and 'id' in event['actor']:
            if event['actor']['id'] == md_id:
                is_match = True
        
        if not is_match and 'payload' in event and 'user' in event['payload'] and event['payload']['user']:
            if 'id' in event['payload']['user'] and event['payload']['user']['id'] == md_id:
                is_match = True
        
        if not is_match and 'repo' in event and 'owner' in event['repo'] and event['repo']['owner']:
            if 'id' in event['repo']['owner'] and event['repo']['owner']['id'] == md_id:
                is_match = True
        
        if is_match:
            matched_count += 1
            filtered_events.append(event)        
    
    return filtered_events

def process_hourly_data(events_generator: Generator[Dict[Any, Any], None, None], md_id: int) -> List[Dict[Any, Any]]:
    return filter_events_by_user(events_generator, md_id)

def events_to_jsonl(events: List[Dict[Any, Any]]) -> str:
    return '\n'.join(json.dumps(event) for event in events)