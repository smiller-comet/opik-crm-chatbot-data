import datetime
import random
import uuid
import opik
from opik import id_helpers
import os
import json
import time

OPIK_API_KEY = os.getenv('OPIK_API_KEY')
OPIK_WORKSPACE = os.getenv('OPIK_WORKSPACE', 'comet-demos')
PROJECT_NAME = "Opik Agent Observability"

# Create Opik client with explicit parameters
client = opik.Opik(api_key=OPIK_API_KEY, workspace=OPIK_WORKSPACE)


def load_traces():
    """Load traces from local JSON file."""
    output_file = "workshop_traces_data.json"
    with open(output_file, 'r') as f:
        loaded_traces = json.load(f)
    return loaded_traces


def load_spans():
    """Load spans from local JSON file."""
    output_file = "workshop_spans_data.json"
    with open(output_file, 'r') as f:
        loaded_spans = json.load(f)
    return loaded_spans


def safe_thread_id(trace):
    """Safely get thread_id from trace dict."""
    try:
        return trace.get('thread_id')
    except (KeyError, AttributeError):
        return None


def parse_datetime(dt_str_or_obj):
    """Parse datetime from string or return datetime object."""
    if isinstance(dt_str_or_obj, str):
        try:
            dt_str = dt_str_or_obj.replace('Z', '+00:00').replace(' ', 'T')
            return datetime.datetime.fromisoformat(dt_str)
        except:
            return None
    elif isinstance(dt_str_or_obj, datetime.datetime):
        return dt_str_or_obj
    return None


def compute_scale_factor(trace):
    """
    Compute the scale factor to compress span offsets into the trace's intended duration.
    
    The trace JSON has a `duration` field (in ms) representing the desired realistic duration,
    but the start_time/end_time timestamps (which spans are aligned to) often span a much
    wider range. This function returns the ratio to compress span offsets proportionally.
    """
    original_start = parse_datetime(trace.get('start_time'))
    original_end = parse_datetime(trace.get('end_time'))
    duration_ms = trace.get('duration', 0)
    
    if not original_start or not original_end or not duration_ms:
        return 1.0
    
    if original_start.tzinfo is None:
        original_start = original_start.replace(tzinfo=datetime.timezone.utc)
    if original_end.tzinfo is None:
        original_end = original_end.replace(tzinfo=datetime.timezone.utc)
    
    original_span_seconds = (original_end - original_start).total_seconds()
    desired_seconds = duration_ms / 1000.0
    
    if original_span_seconds <= 0:
        return 1.0
    
    return desired_seconds / original_span_seconds


def upload_traces_for_day(traces, spans, day_offset, threads_per_day, global_thread_id_map):
    """Upload traces and spans for a specific day (day_offset days ago)."""
    # Backdate to day_offset days ago, using UTC timezone
    day_start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=day_offset)
    # Set to a random time during business hours (9 AM - 5 PM UTC)
    random_hour = random.randint(9, 17)
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)
    day = day_start.replace(hour=random_hour, minute=random_minute, second=random_second, microsecond=0)
    
    # Get threads assigned to this day
    threads_for_day = threads_per_day.get(day_offset, [])
    
    if not threads_for_day:
        print(f"No threads assigned for day {day_offset}, skipping...")
        return 0, 0
    
    # Group traces by thread_id
    traces_by_thread = {}
    for trace in traces:
        thread_id = safe_thread_id(trace)
        if thread_id and thread_id in threads_for_day:
            if thread_id not in traces_by_thread:
                traces_by_thread[thread_id] = []
            traces_by_thread[thread_id].append(trace)
    
    traces_created = 0
    spans_created = 0
    
    # Process all traces for each thread assigned to this day
    for thread_id in threads_for_day:
        if thread_id not in traces_by_thread:
            continue
            
        thread_traces = traces_by_thread[thread_id]
        # Get the day-specific thread_id for this (thread_id, day) combination
        new_thread_id = global_thread_id_map.get((thread_id, day_offset))
        if not new_thread_id:
            continue
        
        # Sort traces by their original start_time to maintain order within thread
        thread_traces.sort(key=lambda t: parse_datetime(t.get('start_time')) or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc))
        
        # Start with a random offset within the day (0-4 hours) to spread threads out
        base_time = day + datetime.timedelta(seconds=random.randint(0, 14400))
        
        for idx, trace in enumerate(thread_traces):
            trace_data_i = trace.copy()
            save_id = trace_data_i['id']
            
            # Compute scale factor: compresses span offsets to fit within intended duration
            scale = compute_scale_factor(trace)
            
            # The desired trace duration comes from the duration field (ms -> seconds)
            desired_duration_s = trace_data_i.get('duration', 2500) / 1000.0
            
            # For subsequent traces in the same thread, add a small delay (30s to 3 min)
            if idx > 0:
                delay_seconds = random.randint(30, 180)
                base_time = base_time + datetime.timedelta(seconds=delay_seconds)
            
            start_time = base_time
            end_time = start_time + datetime.timedelta(seconds=desired_duration_s)
            
            # Move base_time past this trace for the next one
            base_time = end_time
            
            # Generate trace ID with the backdated timestamp using id_helpers
            trace_id = id_helpers.generate_id(timestamp=start_time)
            
            # Prepare trace data
            trace_data_i['id'] = trace_id
            trace_data_i['start_time'] = start_time
            trace_data_i['end_time'] = end_time
            trace_data_i['thread_id'] = new_thread_id
            
            # Remove fields that shouldn't be sent or would conflict
            for key_to_remove in ['feedback_scores', 'duration', 'project_id',
                                  'span_count', 'llm_span_count']:
                trace_data_i.pop(key_to_remove, None)
            
            trace_data_i['project_name'] = PROJECT_NAME
            
            # Create the trace
            trace_obj = client.trace(**trace_data_i)
            traces_created += 1
            
            # Get spans for this trace from the loaded spans list
            trace_spans_list = [span for span in spans if span.get('trace_id') == save_id]
            
            # Compute original trace start for offset calculation
            original_trace_start = parse_datetime(trace.get('start_time'))
            if original_trace_start is None:
                original_trace_start = start_time
            elif original_trace_start.tzinfo is None:
                original_trace_start = original_trace_start.replace(tzinfo=datetime.timezone.utc)
            
            # Generate replacement span IDs using id_helpers with SCALED timestamps
            span_replacement_ids = {}
            for span in trace_spans_list:
                span_id = span.get('id')
                if not span_id:
                    continue
                
                span_start_time = parse_datetime(span.get('start_time'))
                if span_start_time is None:
                    span_start_time = original_trace_start
                elif span_start_time.tzinfo is None:
                    span_start_time = span_start_time.replace(tzinfo=datetime.timezone.utc)
                
                # Scale the offset to fit within desired duration
                raw_offset = (span_start_time - original_trace_start).total_seconds()
                scaled_offset = raw_offset * scale
                adjusted_span_start = start_time + datetime.timedelta(seconds=scaled_offset)
                
                span_replacement_ids[span_id] = id_helpers.generate_id(timestamp=adjusted_span_start)
            
            # Create spans for the trace
            for span in trace_spans_list:
                span_dict = span.copy()
                span_id = span_dict.get('id')
                if not span_id:
                    continue
                
                span_dict['id'] = span_replacement_ids.get(span_id)
                span_dict['parent_span_id'] = span_replacement_ids.get(span_dict.get('parent_span_id'))
                
                # Adjust span start_time: scale offset to fit within trace duration
                if 'start_time' in span_dict and span_dict['start_time']:
                    original_start = parse_datetime(span_dict['start_time'])
                    if original_start and isinstance(original_start, datetime.datetime):
                        if original_start.tzinfo is None:
                            original_start = original_start.replace(tzinfo=datetime.timezone.utc)
                        raw_offset = (original_start - original_trace_start).total_seconds()
                        scaled_offset = raw_offset * scale
                        span_dict['start_time'] = start_time + datetime.timedelta(seconds=scaled_offset)
                    else:
                        span_dict['start_time'] = start_time
                
                # Adjust span end_time: scale offset to fit within trace duration
                if 'end_time' in span_dict and span_dict['end_time']:
                    original_end = parse_datetime(span_dict['end_time'])
                    if original_end and isinstance(original_end, datetime.datetime):
                        if original_end.tzinfo is None:
                            original_end = original_end.replace(tzinfo=datetime.timezone.utc)
                        raw_offset = (original_end - original_trace_start).total_seconds()
                        scaled_offset = raw_offset * scale
                        span_dict['end_time'] = start_time + datetime.timedelta(seconds=scaled_offset)
                    else:
                        span_dict['end_time'] = end_time
                
                # Remove duration from spans too - let Opik calculate from timestamps
                if 'duration' in span_dict:
                    del span_dict['duration']
                
                # Only keep keys that span() accepts
                allowed_keys = {
                    "id",
                    "parent_span_id",
                    "name",
                    "type",
                    "start_time",
                    "end_time",
                    "metadata",
                    "input",
                    "output",
                    "tags",
                    "usage",
                    "model",
                    "provider",
                    "error_info",
                    "total_cost",
                    "attachments"
                }
                
                filtered_span_dict = {k: v for k, v in span_dict.items() if k in allowed_keys}
                
                try:
                    trace_obj.span(**filtered_span_dict)
                    spans_created += 1
                except Exception as e:
                    print(f"Error creating span {span_id}: {e}")
            
            # NOTE: Do NOT call trace_obj.end() here.
            # end() overrides the end_time with the current wall-clock time,
            # which would destroy our backdated timestamps.
    
    print(f"Day {day_offset} ({day.strftime('%Y-%m-%d')}): Created {traces_created} traces and {spans_created} spans")
    return traces_created, spans_created


def main():
    """Main function to upload traces for the last month."""
    print("Loading traces and spans from JSON files...")
    traces = load_traces()
    spans = load_spans()
    print(f"Loaded {len(traces)} traces and {len(spans)} spans")
    
    # Group traces by thread_id
    traces_by_thread = {}
    for trace in traces:
        thread_id = safe_thread_id(trace)
        if thread_id:
            if thread_id not in traces_by_thread:
                traces_by_thread[thread_id] = []
            traces_by_thread[thread_id].append(trace)
    
    unique_thread_ids = list(traces_by_thread.keys())
    print(f"Found {len(unique_thread_ids)} unique threads")
    
    # For each day, sample 3-7 threads
    # Use day-specific thread IDs so threads on different days don't interfere
    threads_per_day = {}
    global_thread_id_map = {}
    
    for day in range(1, 31):
        min_threads = min(3, len(unique_thread_ids))
        max_threads = min(7, len(unique_thread_ids))
        sample_size = random.randint(min_threads, max_threads)
        sampled_threads = random.sample(unique_thread_ids, sample_size)
        threads_per_day[day] = sampled_threads
        
        for thread_id in sampled_threads:
            key = (thread_id, day)
            if key not in global_thread_id_map:
                global_thread_id_map[key] = str(uuid.uuid4())
    
    # Generate traces for the last month (30 days)
    total_traces = 0
    total_spans = 0
    
    start = time.time()
    
    for i in range(1, 31):
        try:
            traces_count, spans_count = upload_traces_for_day(
                traces, spans, i, threads_per_day, global_thread_id_map
            )
            total_traces += traces_count
            total_spans += spans_count
        except Exception as e:
            print(f"Error processing day {i}: {e}")
            import traceback
            traceback.print_exc()
            # Short pause on error, then continue
            time.sleep(2)
    
    # Final flush to ensure all data is sent
    time.sleep(2)
    
    elapsed = time.time() - start
    print(f"\nFinished logging traces for the last month in {elapsed:.1f}s!")
    print(f"Total: {total_traces} traces and {total_spans} spans created")


if __name__ == "__main__":
    main()
