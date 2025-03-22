import multiprocessing as mp
import os
from decimal import Decimal, ROUND_HALF_UP

def process_chunk(chunk_start, chunk_size, filename):
    """Process a chunk of the file and return aggregated results."""
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
                        aggregates[city] = {'min': float('inf'), 
                                          'max': float('-inf'), 
                                          'sum': 0.0, 
                                          'count': 0}
                    city_data = aggregates[city]
                    city_data['min'] = min(city_data['min'], temp)
                    city_data['max'] = max(city_data['max'], temp)
                    city_data['sum'] += temp
                    city_data['count'] += 1
                except (ValueError, IndexError):
                    continue
                    
    except Exception as e:
        return {'error': str(e)}
                
    return aggregates

def merge_results(results):
    """Merge results from all chunks."""
    final = {}
    
    for chunk_result in results:
        if 'error' in chunk_result:
            raise Exception(f"Error in worker process: {chunk_result['error']}")
            
        for city, data in chunk_result.items():
            if city not in final:
                final[city] = {'min': float('inf'), 
                              'max': float('-inf'), 
                              'sum': 0.0, 
                              'count': 0}
            city_data = final[city]
            city_data['min'] = min(city_data['min'], data['min'])
            city_data['max'] = max(city_data['max'], data['max'])
            city_data['sum'] += data['sum']
            city_data['count'] += data['count']
    
    return final

def round_to_one_decimal(value):
    """Round to one decimal place using IEEE 754 round to infinity."""
    return float(Decimal(str(value)).quantize(Decimal('0.1'), rounding=ROUND_HALF_UP))

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    input_file = open(input_file_name, "r", encoding='utf-8')
    output_file = open(output_file_name, "w", encoding='utf-8')
    
    # Get file size
    input_file.seek(0, 2)  # Move to end
    file_size = input_file.tell()
    input_file.seek(0)  # Move back to start
    
    if file_size == 0:
        output_file.close()
        input_file.close()
        return
    
    # Determine chunk size and number of processes
    num_processes = min(mp.cpu_count(), 8)
    chunk_size = max(file_size // num_processes, 1024 * 1024)  # Minimum 1MB chunks
    
    # Create chunk boundaries
    chunks = []
    start = 0
    while start < file_size:
        chunks.append((start, chunk_size))
        start += chunk_size
    
    # Process chunks in parallel
    try:
        with mp.Pool(processes=num_processes) as pool:
            results = pool.starmap(process_chunk, 
                                 [(start, size, input_file_name) for start, size in chunks])
    except Exception as e:
        output_file.close()
        input_file.close()
        raise Exception(f"Multiprocessing error: {str(e)}")
    
    # Merge results
    final_result = merge_results(results)
    
    # Write output with proper rounding and sorting
    sorted_cities = sorted(final_result.keys())
    for city in sorted_cities:
        data = final_result[city]
        min_temp = round_to_one_decimal(data['min'])
        mean_temp = round_to_one_decimal(data['sum'] / data['count'])
        max_temp = round_to_one_decimal(data['max'])
        output_file.write(f"{city}={min_temp}/{mean_temp}/{max_temp}\n")
    
    output_file.close()
    input_file.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        raise