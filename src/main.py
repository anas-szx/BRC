import os
import sys
import mmap
import struct
from collections import defaultdict
import threading
from concurrent.futures import ThreadPoolExecutor

# Constants
FILE = "./testcase.txt"
MIN_TEMP = -999
MAX_TEMP = 999
MAX_NAME_LENGTH = 100
MAX_CITIES = 10000
SEGMENT_SIZE = 1 << 21  # 2MB segments
HASH_TABLE_SIZE = 1 << 17

class Result:
    def __init__(self):
        self.min = MAX_TEMP
        self.max = MIN_TEMP
        self.sum = 0
        self.count = 0
        self.name = None

    def __str__(self):
        avg = (self.sum / 10.0) / self.count if self.count > 0 else 0
        return f"{self.name}={self.min/10.0:.1f}/{avg:.1f}/{self.max/10.0:.1f}"

    def accumulate(self, other):
        self.min = min(self.min, other.min)
        self.max = max(self.max, other.max)
        self.sum += other.sum
        self.count += other.count

class Scanner:
    def __init__(self, mm, start, end):
        self.mm = mm
        self.pos = start
        self.end = end

    def has_next(self):
        return self.pos < self.end

    def get_byte(self, pos=None):
        if pos is None:
            pos = self.pos
        return self.mm[pos] if pos < len(self.mm) else 0

    def add(self, delta):
        self.pos += delta

    def find_delimiter(self, start_pos=None):
        """Find the position of the next semicolon from start_pos"""
        pos = start_pos if start_pos is not None else self.pos
        while pos < self.end:
            if self.mm[pos] == ord(';'):
                return pos
            pos += 1
        return -1

    def find_newline(self, start_pos=None):
        """Find the position of the next newline from start_pos"""
        pos = start_pos if start_pos is not None else self.pos
        while pos < self.end:
            if self.mm[pos] == ord('\n'):
                return pos
            pos += 1
        return self.end

    def scan_number(self):
        """Parse a temperature value (format: ±##.# or ##.#)"""
        pos = self.pos
        sign = 1
        
        # Skip the semicolon
        pos += 1
        
        # Check for negative sign
        if self.mm[pos] == ord('-'):
            sign = -1
            pos += 1
        
        # Parse digits before decimal
        value = 0
        while pos < self.end and self.mm[pos] >= ord('0') and self.mm[pos] <= ord('9'):
            value = value * 10 + (self.mm[pos] - ord('0'))
            pos += 1
        
        # Skip decimal point
        if pos < self.end and self.mm[pos] == ord('.'):
            pos += 1
            
            # Parse single digit after decimal
            if pos < self.end and self.mm[pos] >= ord('0') and self.mm[pos] <= ord('9'):
                value = value * 10 + (self.mm[pos] - ord('0'))
                pos += 1
        else:
            # If no decimal, multiply by 10 as we're working with fixed point
            value *= 10
        
        # Update position and return value
        result = sign * value
        self.pos = pos
        
        return result

    def read_name(self, start_pos, end_pos):
        """Read a city name from start_pos to end_pos"""
        return self.mm[start_pos:end_pos].decode('utf-8')

def process_segment(mm, segment_start, segment_end, file_start, file_end):
    """Process a segment of the file"""
    results = {}
    
    # Adjust segment boundaries to line boundaries
    if segment_start > file_start:
        # Find the first newline after segment_start and start after it
        scanner = Scanner(mm, segment_start, min(segment_start + 1000, file_end))
        nl_pos = scanner.find_newline()
        if nl_pos >= 0:
            segment_start = nl_pos + 1
    
    if segment_end < file_end:
        # Find the last newline before segment_end
        scanner = Scanner(mm, max(segment_start, segment_end - 1000), segment_end)
        nl_pos = scanner.find_newline()
        if nl_pos >= 0:
            segment_end = nl_pos + 1
    
    # Main scanner for the segment
    scanner = Scanner(mm, segment_start, segment_end)
    
    while scanner.has_next():
        # Read city name
        name_start = scanner.pos
        delim_pos = scanner.find_delimiter()
        
        if delim_pos < 0:
            break  # No more valid entries in this segment
            
        name = scanner.read_name(name_start, delim_pos)
        scanner.pos = delim_pos
        
        # Parse temperature value
        temperature = scanner.scan_number()
        
        # Skip to the next line
        nl_pos = scanner.find_newline()
        scanner.pos = nl_pos + 1 if nl_pos >= 0 else scanner.end
        
        # Update results
        if name not in results:
            result = Result()
            result.name = name
            result.min = temperature
            result.max = temperature
            result.sum = temperature
            result.count = 1
            results[name] = result
        else:
            result = results[name]
            result.min = min(result.min, temperature)
            result.max = max(result.max, temperature)
            result.sum += temperature
            result.count += 1
    
    return results

def merge_results(all_results):
    """Merge results from all workers"""
    merged = {}
    
    for results in all_results:
        for name, result in results.items():
            if name not in merged:
                merged[name] = result
            else:
                merged[name].accumulate(result)
    
    return merged

def calculate_average():
    """Main processing function"""
    try:
        with open(FILE, 'r+b') as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            file_size = len(mm)
            
            num_workers = os.cpu_count() or 4
            
            segments = []
            for i in range(0, file_size, SEGMENT_SIZE):
                segment_start = i
                segment_end = min(i + SEGMENT_SIZE, file_size)
                segments.append((segment_start, segment_end))
            
            all_results = []
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(process_segment, mm, start, end, 0, file_size) 
                          for start, end in segments]
                
                for future in futures:
                    all_results.append(future.result())
            
            merged_results = merge_results(all_results)
            
            # Write results directly to output.txt with buffering
            with open('output.txt', 'w', buffering=1<<20) as f:  # 1MB buffer
                buffer = []
                buffer_size = 10000  # Flush every 10K lines
                
                for name, result in sorted(merged_results.items()):
                    min_temp = result.min / 10.0
                    max_temp = result.max / 10.0
                    avg = (result.sum / 10.0) / result.count
                    buffer.append(f"{name}={min_temp:.1f}/{avg:.1f}/{max_temp:.1f}")
                    
                    if len(buffer) >= buffer_size:
                        f.write('\n'.join(buffer) + '\n')
                        buffer.clear()
                
                if buffer:  # Write remaining lines
                    f.write('\n'.join(buffer) + '\n')
            
    except Exception as e:
        return 1
    
    


if __name__ == "__main__":
    sys.exit(calculate_average())
