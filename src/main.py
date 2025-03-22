import math
import mmap
import multiprocessing
from array import array
from io import StringIO

def round_inf(x):
    """Round to one decimal place using ceiling (round to infinity)."""
    return math.ceil(x * 10) / 10

def process_chunk(filename, start_offset, end_offset):
    """Process a chunk of the file and return aggregated results."""
    # Pre-allocate arrays for better memory efficiency
    data = {}
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        size = len(mm)
        
        # Move start_offset to next newline if not at file beginning
        if start_offset != 0:
            while start_offset < size and mm[start_offset] != ord('\n'):
                start_offset += 1
            start_offset += 1
        
        # Extend end_offset to cover the full line
        end = end_offset
        while end < size and mm[end] != ord('\n'):
            end += 1
        if end < size:
            end += 1
        
        chunk = mm[start_offset:end]
        mm.close()
    
    # Process lines in bulk
    lines = chunk.splitlines()
    for line in lines:
        if not line:
            continue
        
        # Fast field separation
        try:
            city, score_str = line.split(b';', 1)
            score = float(score_str)
        except (ValueError, IndexError):
            continue
        
        if city in data:
            stats = data[city]
            if score < stats[0]:
                stats[0] = score
            if score > stats[1]:
                stats[1] = score
            stats[2] += score
            stats[3] += 1
        else:
            data[city] = array('d', [score, score, score, 1])
    
    return data

def merge_data(data_list):
    """Merge results from all chunks into a single dictionary."""
    final = {}
    for data in data_list:
        for city, stats in data.items():
            if city in final:
                final_stats = final[city]
                final_stats[0] = min(stats[0], final_stats[0])
                final_stats[1] = max(stats[1], final_stats[1])
                final_stats[2] += stats[2]
                final_stats[3] += stats[3]
            else:
                final[city] = array('d', stats)
    return final

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    """Main function to process the input file and write the output."""
    # Determine file size using mmap for efficiency
    with open(input_file_name, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = len(mm)
        mm.close()
    
    if file_size == 0:  # Handle empty file case
        open(output_file_name, "w").close()
        return
    
    # Optimize chunk size based on file size
    num_procs = min(multiprocessing.cpu_count() * 4, 32)  # Increased parallelism
    chunk_size = max(file_size // num_procs, 1024 * 1024)  # Minimum 1MB chunks
    chunks = [(i * chunk_size, min((i + 1) * chunk_size, file_size))
                for i in range((file_size + chunk_size - 1) // chunk_size)]
    
    # Process chunks in parallel with larger pool
    with multiprocessing.Pool(num_procs) as pool:
        tasks = [(input_file_name, start, end) for start, end in chunks]
        results = pool.starmap(process_chunk, tasks)
    
    # Merge results efficiently
    final_data = merge_data(results)
    
    # Use StringIO for faster string building
    buffer = StringIO()
    for city in sorted(final_data.keys(), key=lambda c: c.decode()):
        mn, mx, total, count = final_data[city]
        avg = round_inf(total / count)
        buffer.write(f"{city.decode()}={round_inf(mn):.1f}/{avg:.1f}/{round_inf(mx):.1f}\n")
    
    # Single write operation
    with open(output_file_name, "w") as f:
        f.write(buffer.getvalue())

if __name__ == "__main__":
    main()
