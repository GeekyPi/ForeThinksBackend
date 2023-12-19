import sys
import subprocess

# Get the arguments passed from the parent process
args = sys.argv[1:]

# Construct the command that will be run in the terminal
command = f'jdata index -s "NIFTY FIN SERVICE" -f {args[0]} -t {args[1]} -o D:\\ForeThinks\\server\\forethinks\\temp_data\\spot_data\\fnf_data.csv'


# Run the command in the terminal
subprocess.run(command, shell=True)
