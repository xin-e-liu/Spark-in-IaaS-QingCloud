import qingcloud.iaas
import simplejson as json
import pexpect
import time
import datetime
import sys, getopt

master_ip = "1.2.3.4"
username = "user"
password = "password"

num_slaves = 1

def do_job():
	global master_ip
	global username
	global password
	global num_slaves

	print "Number of slaves to instantiate: {0}".format(num_slaves)
	start_time = datetime.datetime.now()
	
	print "########################################################################"
	print "------Connecting to Qing Cloud. The Openstack Cloud------"
	conn = qingcloud.iaas.connect_to_zone(
	        'zone',
	        'key',
	        'key'
	    )

	#ret = conn.describe_instances(status=['running'])
	#all_instances = ret['instance_set']
	#print json.dumps(all_instances, indent=4)

	slave_ips = []
	for i in range(num_slaves):
		instance_name = "vm_slave{0}".format(i+10)

		print "Launing new instance with name {0}...".format(instance_name)
		ret = conn.run_instances(
		        image_id='img-r6sfbxha',
		        instance_type='c2m4',
		        instance_name=instance_name,
		        login_mode="passwd",
		        login_passwd=password,
		        vxnets=["vxnet-0"]
		      )
		print json.dumps(ret, indent=4)

		slave_ip = ""
		while True:
			print "Waiting for instance ready......"
			ret = conn.describe_instances(search_word=instance_name, status=['running'])
			count = ret['total_count']
			if count == 1:
				print "Instance created. Waiting for IP to be ready....."
				slave_ip = ret['instance_set'][0]["vxnets"][0]["private_ip"]
				if slave_ip != None:
					break
			time.sleep(5)

		print "New instance with name {0} create with IP {1}".format(instance_name, slave_ip)
		slave_ips.append(slave_ip)
		time.sleep(1)

	print ""
	print "All instances launch done."
	print "##########################"
	for i in range(num_slaves):
		print "vm_slave{0} --- {1}".format(i+10, slave_ips[i])
	print "##########################"
	print ""
	print "Continue with HDFS and MESOS configurations..."

	print "Connecting to master {0}...".format(master_ip)
	master = pexpect.spawn ('ssh {0}@{1}'.format(username, master_ip), timeout=300)
	master.expect ('.*password:')
	master.sendline ('{0}\r'.format(password))
	master.expect ('.*u3qi9vmd.*')
	print "Connected to master"
	print "Let's first configure HDFS..."

	for slave_ip in slave_ips:
		master.sendline ('echo \'{0}\' >> $HADOOP_INSTALL/etc/hadoop/slaves\r'.format(slave_ip))
		master.expect ('.*ubuntu@i-.*')
	master.sendline ('stop-dfs.sh\r')
	master.expect ('.*secondarynamenode.*where applicable.*')
	time.sleep(1)
	master.sendline ('start-dfs.sh\r')
	master.expect ('.*secondarynamenode.*where applicable.*')
	time.sleep(1)
	print "HDFS config done"
	print ""
	print ""
	print "Continue with MESOS configurations"
	for slave_ip in slave_ips:
		print "Connecting to slave {0}".format(slave_ip)
		master.sendline ('ssh {0}\r'.format(slave_ip))
		master.expect ('.*ubuntu@i-.*')
		print "Connected to slave {0}".format(slave_ip)
		print "Setting up Mesos"
		time.sleep(1)
		master.sendline ('echo {0} | sudo tee /etc/mesos-slave/ip\r'.format(slave_ip))
		master.expect ('.*password.*')
		master.sendline ('{0}\r'.format(password))
		master.expect ('{0}'.format(slave_ip))
		master.sendline ('sudo cp /etc/mesos-slave/ip /etc/mesos-slave/hostname\r')
		master.expect ('.*')
		master.sendline ('sudo service mesos-slave start\r')
		master.expect ('.*')
		print "Mesos done"
		master.sendline ('exit\r')
		master.expect ('.*closed.*ubuntu@i-.*')
		time.sleep(1)


	end_time = datetime.datetime.now()
	diff_time = (end_time - start_time).total_seconds()
	print "########################################################################"
	print "All done. Total time: {0} seconds".format(diff_time)



def parse_args(argv):
	global num_slaves

	try:
		opts, args = getopt.getopt(argv,"hs:",["slave="])
	except getopt.GetoptError:
		print 'qing-hdfs-spark.py -s <numberOfSlaves> '
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print 'qing-hdfs-spark.py -s <numberOfSlaves>'
			sys.exit()
		elif opt in ("-s", "--slave"):
			try:
				num_slaves = int(arg)
			except ValueError:
				print "Number of slaves should be a number"
				sys.exit(2)

	if (num_slaves > 3) or (num_slaves < 1):
		print 'Number of slaves greater than 1 and less than 3'
		sys.exit(2)

if __name__ == "__main__":
	if len(sys.argv) == 1:
		print 'qing-hdfs-spark.py -s <numberOfSlaves> '
		sys.exit(2)
	parse_args(sys.argv[1:])
	do_job()