import math
import mmap
import multiprocessing
import os

def round_inf(x):
    """Round to one decimal place using ceiling (round to infinity)."""
    return math.ceil(x * 10) / 10

def process_chunk(args):
    """Process a chunk of memory-mapped file and return aggregated results."""
    filename, start_offset, end_offset = args
    data = {}
    
    with open(filename, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        size = len(mm)
        
        # Adjust start to next newline
        if start_offset != 0:
            start_offset = mm.find(b'\n', start_offset, min(start_offset + 128, size)) + 1
            if start_offset == 0:  # No newline found
                mm.close()
                return data
        
        # Extend end to next newline
        end = mm.find(b'\n', end_offset, min(end_offset + 128, size)) + 1
        if end == 0:  # No newline found
            end = size
        
        pos = start_offset
        while pos < end:
            # Fast semicolon search
            semi_pos = mm.find(b';', pos, end)
            if semi_pos == -1:
                break
            
            city = mm[pos:semi_pos]
            pos = semi_pos + 1
            
            # Fast number parsing
            num_end = mm.find(b'\n', pos, end)
            if num_end == -1:
                num_end = end
            
            num = 0.0
            sign = 1
            num_bytes = mm[pos:num_end]
            if num_bytes and num_bytes[0] == 45:  # '-'
                sign = -1
                num_bytes = num_bytes[1:]
            
            # Inline float conversion
            if num_bytes:
                num = 0.0
                frac = 0.0
                frac_div = 1.0
                for b in num_bytes:
                    if b == 46:  # '.'
                        frac = num
                        num = 0.0
                        frac_div = 1.0
                    else:
                        digit = b - 48  # '0' = 48
                        if digit < 10:
                            if frac_div == 1.0:
                                num = num * 10 + digit
                            else:
                                frac_div *= 0.1
                                num += digit * frac_div
                num = (frac + num) * sign
            
            # Update stats with tuple
            stats = data.get(city)
            if stats:
                mn, mx, total, count = stats
                data[city] = (min(mn, num), max(mx, num), total + num, count + 1)
            else:
                data[city] = (num, num, num, 1)
            
            pos = num_end + 1
        
        mm.close()
    
    return data

def merge_data(data_list):
    """Merge results from all chunks into a single dictionary."""
    final = {}
    for data in data_list:
        for city, stats in data.items():
            if city in final:
                f_mn, f_mx, f_total, f_count = final[city]
                mn, mx, total, count = stats
                final[city] = (min(f_mn, mn), max(f_mx, mx), f_total + total, f_count + count)
            else:
                final[city] = stats
    return final

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    """Main function to process the input file and write the output."""
    # Get file size and validate
    fd = os.open(input_file_name, os.O_RDONLY)
    file_size = os.fstat(fd).st_size
    if file_size == 0:
        os.close(fd)
        os.write(os.open(output_file_name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC), b"")
        return
    
    # Optimize process count and chunk size
    cpu_count = multiprocessing.cpu_count()
    target_chunk_size = 16 * 1024 * 1024  # 16MB for better I/O efficiency
    num_procs = max(1, min(cpu_count * 4, file_size // target_chunk_size))
    chunk_size = file_size // num_procs
    
    # Create chunk boundaries
    chunks = [(i * chunk_size, (i + 1) * chunk_size if i < num_procs - 1 else file_size)
              for i in range(num_procs)]
    tasks = [(input_file_name, start, end) for start, end in chunks]
    
    # Process chunks in parallel with optimized pool
    with multiprocessing.Pool(processes=num_procs, maxtasksperchild=1) as pool:
        results = pool.map(process_chunk, tasks, chunksize=1)
    
    os.close(fd)
    
    # Merge results
    final_data = merge_data(results)
    
    # Optimized buffered output
    out_fd = os.open(output_file_name, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    buffer = bytearray()
    buffer_size = 128 * 1024  # 128KB buffer
    
    for city in sorted(final_data, key=bytes.decode):
        mn, mx, total, count = final_data[city]
        avg = total / count
        line = f"{city.decode()}={round_inf(mn):.1f}/{round_inf(avg):.1f}/{round_inf(mx):.1f}\n".encode()
        buffer.extend(line)
        
        if len(buffer) >= buffer_size:
            os.write(out_fd, buffer)
            buffer.clear()
    
    if buffer:
        os.write(out_fd, buffer)
    
    os.close(out_fd)

if __name__ == "__main__":
    main()