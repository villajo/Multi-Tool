import sys
import yaml
import paramiko
import time
import random
from datetime import datetime
from termcolor import colored, cprint


def bounce_ch_servers(clusterfile,ssh_client, group, location):

   if group == 'cluster':
      systems = clusterfile['clusters'][location].split(" ")
   elif group == 'systems':
      systems = clusterfile['systems'][location].split(" ")

   if len(group) == 0 or len(systems) == 0:
      sys.exit()

   for system in systems:
      print("Restarting Clickhouse Server on : " + system + "." + clusterfile['config']['domain'])
      ssh_client.connect(system + "." + clusterfile['config']['domain'])
      stdin, stdout, stderr = ssh_client.exec_command('sudo systemctl stop clickhouse-server')
      if stdout:
         print(stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t').replace('\"',"").strip())
      if stderr:
         print(stderr.read().decode().replace('\\n', '\n').replace('\\t', '\t').replace('\"',"").strip())
      time.sleep(3)
      stdin, stdout, stderr = ssh_client.exec_command('sudo systemctl start clickhouse-server')
      if stdout:
         print(stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t').replace('\"',"").strip())
      if stderr:
         print(stderr.read().decode().replace('\\n', '\n').replace('\\t', '\t').replace('\"',"").strip()) 
      time.sleep(10)
      ssh_client.close()

def get_create_table(clusterfile, ssh_client, location, database, table):
   systems = clusterfile['clusters'][location].split(" ")
   systems_length = random.randrange(len(systems))
   system = systems[systems_length] + "." + clusterfile['config']['domain']
   ssh_client.connect(system)
   _,stdout,_ = ssh_client.exec_command('clickhouse-client --database=' + database + ' --query="SHOW CREATE ' + table + '" ')
   return stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t')

def get_database_tables(clusterfile, ssh_client, location, database):
   systems = clusterfile['clusters'][location].split(" ")
   systems_length = random.randrange(len(systems))
   system = systems[systems_length] + "." + clusterfile['config']['domain']
   ssh_client.connect(system)
   _,stdout,_ = ssh_client.exec_command('clickhouse-client --database=' + database + ' --query="show tables"')
   return stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t').replace('\"',"").strip()

def get_databases(clusterfile, ssh_client, location):
   systems = clusterfile['clusters'][location].split(" ")
   systems_length = random.randrange(len(systems))
   system = systems[systems_length] + "." + clusterfile['config']['domain']
   ssh_client.connect(system)
   _,stdout,_ = ssh_client.exec_command('clickhouse-client --query="show databases"')
   return stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t').replace('\"',"").replace('default','').replace('information_schema','').replace('system','').replace('INFORMATION_SCHEMA','').strip()

def get_chcopy_jobs(clusterfile, ssh_client, group, location):
   print("Current time in UTC is: " + colored(str(datetime.utcnow()), "green") )
   if group == 'cluster':
      systems = clusterfile['clusters'][location].split(" ")
   elif group == 'systems':
      systems = clusterfile['systems'][location].split(" ")

   if len(group) == 0 or len(systems) == 0:
      sys.exit()

   for system in systems:
      ssh_client.connect(system + "." + clusterfile['config']['domain'])
      _, stdout, _= ssh_client.exec_command('ls -al /tmp | grep clickhouse-copier')
      returned_line = stdout.read().decode()
      if len(returned_line) > 1:
         print("Found Clickhouse Copy jobs in " + location.capitalize() + " on : " + system + "." + clusterfile['config']['domain'])
         print(returned_line.strip())
      ssh_client.close()

def get_table_ttl(clusterfile, ssh_client, location, database, table):
   systems = clusterfile['clusters'][location].split(" ")
   systems_length = random.randrange(len(systems))
   system = systems[systems_length] + "." + clusterfile['config']['domain']
   ssh_client.connect(system)
   _,stdout,_ = ssh_client.exec_command('clickhouse-client --database=' + database + ' --query="SHOW CREATE ' + table + '"')
   returned_output = stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t')
   for line in returned_output:
      if "TTL" in line:
         return line
         break
   return "No TTL Found on " + database + "." + table
   #print(stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t'))

def get_tc_branch(clusterfile, ssh_client, group, location):
   return_data = []

   if group == 'cluster':
      systems = clusterfile['clusters'][location].split(" ")
   elif group == 'systems':
      systems = clusterfile['systems'][location].split(" ")

   if len(group) == 0 or len(systems) == 0:
      sys.exit()

   for system in systems:
      ssh_client.connect(system + "." + clusterfile['config']['domain'])
      stdin, stdout, stderr = ssh_client.exec_command('sudo cat /opt/ansible/inventory.override.yml | grep tc_branch: ')
      if stdout:
         return_data.append(system + "." + clusterfile['config']['domain'] + ' - ' + stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t').replace('\"',"").strip())
      ssh_client.close()
      
   return return_data

def print_all_table_ttls(clusterfile, ssh_client, location):
   databases = get_databases(clusterfile, ssh_client, location).split('\n')
   for database in databases:
      print("-----" + database + "-----")
      tables = get_database_tables(clusterfile, ssh_client, location, database).split('\n')
      for table in tables:
         result = get_table_ttl(clusterfile, ssh_client, location, database, table)
         if "No TTL Found on " in result:
            print(result)
         else: 
            print(database + "." + table + " is set to " + result) 


#Find Pieces.. 
def find_all_pieces_on_cluster(clusterfile, ssh_client):
   for location in clusterfile['clusters']:
      systems = clusterfile['clusters'][location].split(" ")
      for system in systems:
         ssh_client.connect(system + '.' + clusterfile['config']['domain'])
         _,stdout,_ = ssh_client.exec_command('clickhouse-client --query="show databases"')
         databases = stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t').replace('\"',"").replace('default','').replace('information_schema','').replace('system','').replace('INFORMATION_SCHEMA','').strip().split("\n")
         for database in databases:
            conn_string = 'clickhouse-client --database=' + database + ' --query="show tables"'
            _,stdout,_ = ssh_client.exec_command(conn_string)
            tables = stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t').replace('\"',"").strip().split("\n")
            for table in tables:
               if "_piece_" in table:
                  print(system + ' has a clickhouse copy piece located at ' + database + '.' + table) 

#Yeah, I'm lazy.. I know I could have built this into another function.. This should work though. 
def remove_all_pieces_on_cluster(clusterfile, ssh_client):
   for location in clusterfile['clusters']:
      systems = clusterfile['clusters'][location].split(" ")
      for system in systems:
         ssh_client.connect(system + '.' + clusterfile['config']['domain'])
         _,stdout,_ = ssh_client.exec_command('clickhouse-client --query="show databases"')
         databases = stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t').replace('\"',"").replace('default','').replace('information_schema','').replace('system','').replace('INFORMATION_SCHEMA','').strip().split("\n")
         for database in databases:
            conn_string = 'clickhouse-client --database=' + database + ' --query="show tables"'
            _,stdout,_ = ssh_client.exec_command(conn_string)
            tables = stdout.read().decode().replace('\\n', '\n').replace('\\t', '\t').replace('\"',"").strip().split("\n")
            for table in tables:
               if "_piece_" in table:
                  _,_,_ = ssh_client.exec_command('sudo touch /opt/data/clickhouse/flags/force_drop_table')
                  _,_,_ = ssh_client.exec_command('clickhouse-client --database=' + database + ' --query="DROP TABLE ' + table +'"')
                  _,_,_ = ssh_client.exec_command('sudo rm -f /opt/data/clickhouse/flags/force_drop_table')

def print_database_tables(clusterfile, ssh_client, location, database):
   print(get_database_tables(clusterfile, ssh_client, location, database))

def print_databases(clusterfile, ssh_client, location):
   print(get_databases(clusterfile, ssh_client, location))

def print_table_ttl(clusterfile, ssh_client, location, database, table):
   print(get_table_ttl(clusterfile, ssh_client, location, database, table))

def print_create_statement(clusterfile, ssh_client, location, database, table):
   print(get_create_table(clusterfile, ssh_client, location, database, table))

def print_tc_branch(clusterfile, ssh_client, group, location):
   print("Getting values please wait..")
   output = get_tc_branch(clusterfile, ssh_client, group, location)
   for entry in output:
      print(entry)

def set_single_table_ttl(clusterfile, ssh_client, database, table):
   for location in clusterfile['clusters']:
      systems = clusterfile['clusters'][location].split(" ")
      systems_length = random.randrange(len(systems))
      system = systems[systems_length] + "." + clusterfile['config']['domain']
      ssh_client.connect(system)
      query = 'clickhouse-client --database=' + database + ' --query="EXISTS ' + database + '.' + table + '"'
      _,stdout,_ = ssh_client.exec_command(query)
      output = int(stdout.read().decode().strip())
      if output == 1:
         if clusterfile['db_settings'][database][table]['ttl'] != None:
            ttl_query = 'clickhouse-client --database=' + database + ' --query="ALTER TABLE ' + database + '.' + table + ' ON CLUSTER ' + clusterfile['synonyms']['cluster'][location] + ' MODIFY TTL ' + clusterfile['db_settings'][database][table]['ttl']
            #_,_,_ = ssh_client.exec_command(ttl_query)
            print(ttl_query)
         else:
            print("No TTL Whatsoever")
            ttl_query = 'clickhouse-client --database=' + database + ' --query="ALTER TABLE ' + database + '.' + table + ' ON CLUSTER ' + clusterfile['synonyms']['cluster'][location] + ' REMOVE TTL'
            #_,_,_ = ssh_client.exec_command(ttl_query)
            print(ttl_query)

      ssh_client.close()

def set_tc_branch(clusterfile, ssh_client, group, location, tc_branch):
   if group == 'cluster':
      systems = clusterfile['clusters'][location].split(" ")
   elif group == 'systems':
      systems = clusterfile['systems'][location].split(" ")

   if len(group) == 0 or len(systems) == 0:
      sys.exit()

   for system in systems:
      print("Setting Inventory Override Branch Value for Clickhouse Server on : " + system + "." + clusterfile['config']['domain'])
      ssh_client.connect(system + "." + clusterfile['config']['domain'])
      
      stdin, stdout, stderr = ssh_client.exec_command('sudo cat /opt/ansible/inventory.override.yml | grep tc_branch: ')
      if stdout:
         old_branch = stdout.read().decode().strip()
         if old_branch != 'tc_branch: ' + tc_branch: 
            change_string = 'sudo sed -i \'s/' + old_branch + '/tc_branch: ' + tc_branch + '/g\' /opt/ansible/inventory.override.yml'
            _, _, _, = ssh_client.exec_command(change_string) 
            stdin, stdout, stderr = ssh_client.exec_command('sudo cat /opt/ansible/inventory.override.yml | grep tc_branch: ')
            if stdout:
               new_branch = stdout.read().decode().strip()
               print("Old Branch: " + old_branch)
               print("New Branch: " + new_branch)
         else:
            print("Branch already set.. Skipping")
      ssh_client.close()

def drop_table_gt_50(clusterfile, ssh_client, group, location, database, table):
   if group == 'cluster':
      systems = clusterfile['clusters'][location].split(" ")
   elif group == 'systems':
      systems = clusterfile['systems'][location].split(" ")

   if len(group) == 0 or len(systems) == 0:
      sys.exit()

   for system in systems:
      print("Removing " + database + "." + table + " from Clickhouse Server on : " + system + "." + clusterfile['config']['domain'])
      ssh_client.connect(system + "." + clusterfile['config']['domain'])
      _,_,_ = ssh_client.exec_command('sudo touch /opt/data/clickhouse/flags/force_drop_table')
      _,_,_ = ssh_client.exec_command('clickhouse-client --database=' + database + ' --query="DROP TABLE ' + table +'"')
      _,_,_ = ssh_client.exec_command('sudo rm -f /opt/data/clickhouse/flags/force_drop_table')

def main():
   #argumentList = sys.argv[1:]
   ssh_client = paramiko.client.SSHClient()
   ssh_client.load_system_host_keys()

   with open(sys.argv[1], "r") as stream:
      try:
         clusterfile = yaml.safe_load(stream)
      except yaml.YAMLError as exc:
            print(exc)



   if sys.argv[2] == "restart":
      if sys.argv[4] != None and sys.argv[3] != None:
         bounce_ch_servers(clusterfile, ssh_client, sys.argv[3], sys.argv[4])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION> <CLUSTER/SYSTEMS> <LOCATION>")

   elif sys.argv[2] == "print_tc_branch":
      if sys.argv[4] != None and sys.argv[3] != None:
         print_tc_branch(clusterfile, ssh_client, sys.argv[3], sys.argv[4])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION> <CLUSTER/SYSTEMS> <LOCATION>")

   elif sys.argv[2] == "get_create_table":
      if sys.argv[4] != None:
         get_table_ttl(clusterfile, ssh_client, sys.argv[3], sys.argv[4], sys.argv[5])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION> <LOCATION> <DATABASE> <TABLE>")
   elif sys.argv[2] == "get_database_tables":
      if sys.argv[4] != None:
         get_database_tables(clusterfile, ssh_client, sys.argv[3], sys.argv[4])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION> <LOCATION> <DATABASE>")
   elif sys.argv[2] == "print_databases":
      if sys.argv[3] != None:
         print_databases(clusterfile, ssh_client, sys.argv[3])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION> <LOCATION>")
   elif sys.argv[2] == "get_table_ttl":
      if sys.argv[4] != None:
         print_table_ttl(clusterfile, ssh_client, sys.argv[3], sys.argv[4], sys.argv[5])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION> <LOCATION> <DATABASE> <TABLE>")
   elif sys.argv[2] == "get_all_pieces_on_cluster":
         find_all_pieces_on_cluster(clusterfile, ssh_client)
         #Example - python3 mtool.py ott_nextgen.yaml get_all_pieces_on_cluster ashburn
#TODO = get_all_table_ttls
   elif sys.argv[2] == "print_all_table_ttls":
      if sys.argv[3] != None:
         print_all_table_ttls(clusterfile, ssh_client, sys.argv[3])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION>")

#TODO - set_single_table_ttl
   elif sys.argv[2] == "set_single_table_ttl":
      if sys.argv[4] != None and sys.argv[3] != None:
         set_single_table_ttl(clusterfile, ssh_client, sys.argv[3], sys.argv[4])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION> <DATABASE> <TABLE>")

#TODO - get_all_pieces
   elif sys.argv[2] == "get_all_pieces":
      if sys.argv[4] != None and sys.argv[3] != None:
         get_all_pieces(clusterfile, ssh_client, sys.argv[3])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION> <CLUSTER/SYSTEMS>")

#chcopy jobs
   elif sys.argv[2] == "get_chcopy_jobs":
      if sys.argv[4] != None and sys.argv[3] != None:
         get_chcopy_jobs(clusterfile, ssh_client, sys.argv[3], sys.argv[4])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION> <CLUSTER/SYSTEMS> <LOCATION>")
   elif sys.argv[2] == "tc_branch_set":
      if sys.argv[5] != None:
         set_tc_branch(clusterfile, ssh_client, sys.argv[3], sys.argv[4], sys.argv[5])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION> <CLUSTER/SYSTEMS> <LOCATION> <TC_BRANCH>")
   elif sys.argv[2] == "purge_large_table":
      if sys.argv[5] != None and sys.argv[6] != None:
         drop_table_gt_50(clusterfile, ssh_client, sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
      else:
         print("usage: python3 mtool.py <FILE> <FUNCTION> <CLUSTER/SYSTEMS> <LOCATION> <DATABASE> <TABLE>")
   else:
      print("usage: python3 mtool.py <FILE> <FUNCTION> <CLUSTER/SYSTEMS> <LOCATION>") 

if __name__ == '__main__':
   main()
