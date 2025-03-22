import multiprocessing as mp
from collections import defaultdict

def process_chunk(chunk_start, chunk_size, filename):
    """Process a chunk of the file and return aggregated results."""
    # Use dict instead of defaultdict for better serialization
    aggregates = {}
    
    try:
        with open(filename, "r", encoding='utf-8') as input_file:
            input_file.seek(chunk_start)
            
            if chunk_start > 0:
                input_file.readline()  # Skip to next full line
            
            chunk = input_file.read(chunk_size)
            
            if not chunk:
                return aggregates
            
            lines = chunk.splitlines()
            for line in lines:
                if not line:
                    continue
                try:
                    city, temp_str = line.split(';')
                    temp = float(temp_str)
                    
                    if city not in aggregates:
                        aggregates[city] = {
                            'min': temp,
                            'max': temp,
                            'sum': temp,
                            'count': 1
                        }
                    else:
                        city_data = aggregates[city]
                        city_data['min'] = min(city_data['min'], temp)
                        city_data['max'] = max(city_data['max'], temp)
                        city_data['sum'] += temp
                        city_data['count'] += 1
                except (ValueError, IndexError):
                    continue
                    
        return aggregates
    except Exception as e:
        print(f"Error processing chunk: {e}")
        return {}

def merge_results(results):
    """Merge results from all chunks."""
    final = {}
    
    for chunk_result in results:
        for city, data in chunk_result.items():
            if city not in final:
                final[city] = data.copy()
            else:
                final[city]['min'] = min(final[city]['min'], data['min'])
                final[city]['max'] = max(final[city]['max'], data['max'])
                final[city]['sum'] += data['sum']
                final[city]['count'] += data['count']
    
    return final

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    try:
        with open(input_file_name, "r", encoding='utf-8') as input_file:
            input_file.seek(0, 2)  # Move to end
            file_size = input_file.tell()
            
            if file_size == 0:
                with open(output_file_name, "w", encoding='utf-8') as output_file:
                    return
            
            # Determine chunk size and number of processes
            num_processes = mp.cpu_count()
            chunk_size = max(file_size // num_processes, 1024 * 1024)  # Minimum 1MB chunks
            
            # Create chunk boundaries
            chunks = []
            start = 0
            while start < file_size:
                chunks.append((start, chunk_size))
                start += chunk_size
        
        # Process chunks in parallel
        with mp.Pool(processes=num_processes) as pool:
            results = pool.starmap(process_chunk, 
                                 [(start, size, input_file_name) for start, size in chunks])
        
        # Merge results
        final_result = merge_results(results)
        
        # Write output with one city per line
        with open(output_file_name, "w", encoding='utf-8') as output_file:
            for city in sorted(final_result.keys()):
                data = final_result[city]
                avg = data['sum'] / data['count']
                output_file.write(f"{city}={data['min']:.1f}/{avg:.1f}/{data['max']:.1f}\n")
                
    except Exception as e:
        raise

if __name__ == "__main__":
    main()
