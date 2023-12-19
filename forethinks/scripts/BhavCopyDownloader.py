import sys
import subprocess

# Get the arguments passed from the parent process
args = sys.argv[1:]

# Construct the command that will be run in the terminal
command = f'jdata bhavcopy -d D:\\ForeThinks\\server\\forethinks\\temp_data -f {args[0]} -t {args[1]} --fo'

# Run the command in the terminal
subprocess.run(command, shell=True)
