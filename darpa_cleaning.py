from subprocess import Popen, PIPE
import os
import re

def modified_log(log_dir, filename):
 
 print("Cleaning {}".format(filename))
 log_file = os.path.join(log_dir, filename)
 clean_file = os.path.join(log_dir, filename + '.csv')
 with open(log_file) as original:
  lines = original.readlines()
 with open(clean_file, 'w') as cleaned:
  del lines[0:6]
  a= lines[0].split('	',1)[1]
  lines.insert(0,a)
  del lines[1:3]
  lines.pop()
 lines = [sub.replace('	', ',') for sub in lines]
 with open(clean_file, "w") as sources:
  for line in lines:
   sources.write(line)
 
 return 0
def main():
 base_dir = os.getcwd()
 data_dir = os.path.join(base_dir, "darpa_log_files")
 logs = ['conn.log','dns.log','http.log','weird.log']
 
 for log in logs:
  modified_log(data_dir, log)
if __name__ == '__main__':
 main()
