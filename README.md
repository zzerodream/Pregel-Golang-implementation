# Mini Pregel

### VM configuration
- install ubuntu 22.04
- install go
- install other tools like net-tools, openssh_server (for scp to copy file from host to VM)   
- Exit VM, set network adapter using host-only adapter in virtualbox GUI  
- Start VM again.  


### How to run

I am using three VMs  
- 1. hardcode IPADD and IPADD_R in main.go {workerID: ip_addr} and {ip_addr: workerID}, run ifconfig to check ip   
- 2. change MASTERIP in main.go accordingly.  
- 3. git clone/scp the code to VM  
For master go run . master.  
For worker1: go run . worker 1                    
For worker2: go run . worker 2   