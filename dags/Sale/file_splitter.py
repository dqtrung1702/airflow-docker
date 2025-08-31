import os
import pandas as pd

class FileSplitter:
    def __init__(self, input_file, output_dir, num_chunks=5):
        self.input_file = input_file
        self.output_dir = output_dir
        self.num_chunks = num_chunks
        
    def split_file(self):
        """Split large CSV file into fixed number of chunks."""
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Read total number of rows
        total_rows = sum(1 for _ in open(self.input_file)) - 1  # Subtract header row
        rows_per_chunk = total_rows // self.num_chunks
        
        # Get base filename without extension
        base_name = os.path.splitext(os.path.basename(self.input_file))[0]
        
        chunk_files = []
        for i in range(self.num_chunks):
            skiprows = i * rows_per_chunk + 1 if i > 0 else 0
            nrows = rows_per_chunk if i < self.num_chunks - 1 else None
            
            # Read chunk
            chunk = pd.read_csv(
                self.input_file, 
                skiprows=skiprows, 
                nrows=nrows,
                header=0 if i == 0 else None,
                names=pd.read_csv(self.input_file, nrows=0).columns
            )
            
            # Write chunk to file
            output_file = os.path.join(
                self.output_dir, 
                f"{base_name}_part_{i+1:03d}.csv"
            )
            chunk.to_csv(output_file, index=False)
            chunk_files.append(output_file)
            
            
        return chunk_files