#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "coordinator.grpc.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::GetSlaveRequset;
using csce438::ID;
using csce438::GetAllServersRequset;
using csce438::GetAllServersResponse;

std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

struct zNode{
    int serverID;
    int clusterID;
    std::string hostname;
    std::string port;
    std::string type; // type can be either down or alive
    std::time_t last_heartbeat;

    // synchronizer | master | slave
    bool isMaster;
    bool isSynchronizer;

    bool missed_heartbeat;
    bool isActive();
    bool isServer();

};

bool zNode::isActive(){
    bool status = difftime(getTimeNow(),last_heartbeat) < 10;
    std::cout<<serverID<<" "<<status<<std::endl;
    return status;
}

// Functions as a server, not as a synchronizer
bool zNode::isServer(){
    return !isSynchronizer;
}

//potentially thread safe
std::mutex v_mutex;
std::vector<zNode> cluster1;
std::vector<zNode> cluster2;
std::vector<zNode> cluster3;


//func declarations
int findServer(std::vector<zNode> v, int id);
std::time_t getTimeNow();
void checkHeartbeat();

class CoordServiceImpl final : public CoordService::Service {

  Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
    // Lock the mutex to ensure thread safety
    v_mutex.lock();

    int id=serverinfo->clusterid();
    int serverId=serverinfo->serverid();
    int i = 0, j = 0, k = 0;

    // Update the last heartbeat time for the corresponding server in the appropriate cluster
    switch (id) {
      case 1: 
        while (i < cluster1.size()) {
          if (serverId == cluster1[i].serverID) {
            cluster1[i].last_heartbeat = getTimeNow();
            break;
          }
          ++i;
        }
        break;
      case 2:
        while (j < cluster2.size()) {
          if (serverId == cluster2[j].serverID) {
            cluster2[j].last_heartbeat = getTimeNow();
            break;
          }
          ++j;
        }
        break;
      case 3:
        while (k < cluster3.size()) {
          if (serverId == cluster3[k].serverID) {
            cluster3[k].last_heartbeat = getTimeNow();
            break;
          }
          ++k;
        }
        break;
    }

    confirmation->set_status(true);

    // Unlock the mutex to release the lock
    v_mutex.unlock();
    return Status::OK;
  }
  
  void set_server_parameters(ServerInfo* serverinfo,zNode &node,int cid){
    serverinfo->set_clusterid((int32_t)cid);
    serverinfo->set_serverid((int32_t)node.serverID);
    serverinfo->set_hostname(node.hostname);
    serverinfo->set_port(node.port);
    serverinfo->set_type("alive");
    serverinfo->set_servertype(node.isSynchronizer ? "synchronizer" : "server");
  }

  // This function retrieves server information for the requested client ID. 
  // It assumes the existence of three clusters and has hardcoded mathematical values 
  // to represent this assumption.
  Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
    int userID = id->id();
    int clusterID = ((userID-1)%3)+1;

    serverinfo->set_type("down");

    switch (clusterID) {
    case 1: {
        int i = 0;
        bool find = false;

        // Search for a master server in Cluster 1
        while (i < cluster1.size()) {
            if (cluster1[i].isMaster) {
                std::cout << "Locate master, cluster1 - server" << cluster1[i].serverID << std::endl;
                set_server_parameters(serverinfo, cluster1[i], clusterID);

                find = true;
                break;
            }
            ++i;
        }

        // If no master is found, select a new master server
        if (!find) {
            i = 0;
            while (i < cluster1.size()) {
                if (cluster1[i].type == "alive" && cluster1[i].isServer()) {
                    std::cout << "Latest master, cluster1 - server" << cluster1[i].serverID << std::endl;
                    cluster1[i].isMaster = true;
                    set_server_parameters(serverinfo, cluster1[i], clusterID);
                    break;
                }
                ++i;
            }
        }
        break;
    }

    case 2: {
        int i = 0;
        bool find = false;

        // Search for a master server in Cluster 2
        while (i < cluster2.size()) {
            if (cluster2[i].isMaster) {
                std::cout << "Locate master, cluster2 - server" << cluster2[i].serverID << std::endl;
                set_server_parameters(serverinfo, cluster2[i], clusterID);
                find = true;
                break;
            }
            ++i;
        }

        // If no master is found, select a new master server
        if (!find) {
            i = 0;
            while (i < cluster2.size()) {
                if (cluster2[i].type == "alive" && cluster2[i].isServer()) {
                    std::cout << "Latest master, cluster2 - server" << cluster2[i].serverID << std::endl;
                    cluster2[i].isMaster = true;
                    set_server_parameters(serverinfo, cluster2[i], clusterID);
                    break;
                }
                ++i;
            }
        }
        break;
    }

    case 3: {
        int i = 0;
        bool find = false;

        // Search for a master server in Cluster 3
        while (i < cluster3.size()) {
            if (cluster3[i].isMaster) {
                std::cout << "Locate master, cluster3 - server" << cluster3[i].serverID << std::endl;
                set_server_parameters(serverinfo, cluster3[i], clusterID);
                find = true;
                break;
            }
            ++i;
        }

        // If no master is found, select a new master server
        if (!find) {
            i = 0;
            while (i < cluster3.size()) {
                if (cluster3[i].type == "alive" && cluster3[i].isServer()) {
                    std::cout << "Latest master, cluster3 - server" << cluster3[i].serverID << std::endl;
                    cluster3[i].isMaster = true;
                    set_server_parameters(serverinfo, cluster3[i], clusterID);
                    break;
                }
                ++i;
            }
        }
        break;
    }
    }
    return Status::OK;
  }

  // Upon server startup, it invokes this function to register itself with the coordinator.
  Status Create(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
    zNode node=*(new zNode());

    // Initialize zNode with server information
    node.hostname=serverinfo->hostname();
    node.last_heartbeat=getTimeNow();
    node.serverID=serverinfo->serverid();
    node.clusterID=serverinfo->clusterid();

    node.port=serverinfo->port();
    node.type="alive";

    if(serverinfo->servertype()=="synchronizer" || serverinfo->servertype()!="server"){
      std::cout<<"Latest Synchronizer "<<"("<<serverinfo->clusterid()<<") - ("<<serverinfo->serverid()<<")"<<std::endl;
      node.isSynchronizer=true;
    }

    int cid = serverinfo->clusterid();

    switch (cid) {
    case 1: {
        bool find = false;
        int i = 0;

        // Search for the server in Cluster 1
        while (i < cluster1.size()) {
            if (cluster1[i].serverID == node.serverID && cluster1[i].isSynchronizer == node.isSynchronizer) {
                std::cout << "Renew Node" << "(" << serverinfo->clusterid() << ") - (" << serverinfo->serverid() << ")" << std::endl;
                node.type = "alive";
                find = true;
                break;
            }
            ++i;
        }

        // If not found, add a new node to Cluster 1
        if (!find) {
            std::cout << "Latest Node" << "(" << serverinfo->clusterid() << ") - (" << serverinfo->serverid() << ")" << std::endl;
            cluster1.push_back(node);
        }
        break;
    }

    case 2: {
        bool find = false;
        int i = 0;

        // Search for the server in Cluster 2
        while (i < cluster2.size()) {
            if (cluster2[i].serverID == node.serverID && cluster2[i].isSynchronizer == node.isSynchronizer) {
                std::cout << "Renew Node" << "(" << serverinfo->clusterid() << ") - (" << serverinfo->serverid() << ")" << std::endl;
                node.type = "alive";
                find = true;
                break;
            }
            ++i;
        }

        // If not found, add a new node to Cluster 2
        if (!find) {
            std::cout << "Latest Node" << "(" << serverinfo->clusterid() << ") - (" << serverinfo->serverid() << ")" << std::endl;
            cluster2.push_back(node);
        }
        break;
    }

    case 3: {
        bool find = false;
        int i = 0;

        // Search for the server in Cluster 3
        while (i < cluster3.size()) {
            if (cluster3[i].serverID == node.serverID && cluster3[i].isSynchronizer == node.isSynchronizer) {
                std::cout << "Renew Node" << "(" << serverinfo->clusterid() << ") - (" << serverinfo->serverid() << ")" << std::endl;
                node.type = "alive";
                find = true;
                break;
            }
            ++i;
        }

        // If not found, add a new node to Cluster 3
        if (!find) {
            std::cout << "Latest Node" << "(" << serverinfo->clusterid() << ") - (" << serverinfo->serverid() << ")" << std::endl;
            cluster3.push_back(node);
        }
        break;
    }
    }
   
    return Status::OK;
  }
  
  Status GetSlave(ServerContext* context, const GetSlaveRequset* getSlaveRequest, ServerInfo* serverinfo) override {
    int id = getSlaveRequest->clusterid();

    switch (id) {
    case 1: {
        int i = 0;

        // Search for a non-master server in Cluster 1
        while (i < cluster1.size()) {
            if (!cluster1[i].isMaster) {

                // Set server information based on the found non-master server
                serverinfo->set_clusterid(id);
                serverinfo->set_serverid(cluster1[i].serverID);
                serverinfo->set_hostname(cluster1[i].hostname);
                serverinfo->set_port(cluster1[i].port);
                serverinfo->set_type(cluster1[i].type);
                break;
            }
            ++i;
        }
        break;
    }

    case 2: {
        int i = 0;

        // Search for a non-master server in Cluster 2
        while (i < cluster2.size()) {
            if (!cluster2[i].isMaster) {

                // Set server information based on the found non-master server
                serverinfo->set_clusterid(id);
                serverinfo->set_serverid(cluster2[i].serverID);
                serverinfo->set_hostname(cluster2[i].hostname);
                serverinfo->set_port(cluster2[i].port);
                serverinfo->set_type(cluster2[i].type);
                break;
            }
            ++i;
        }
        break;
    }

    case 3: {
        int i = 0;

        // Search for a non-master server in Cluster 3
        while (i < cluster3.size()) {
            if (!cluster3[i].isMaster) {

                // Set server information based on the found non-master server
                serverinfo->set_clusterid(id);
                serverinfo->set_serverid(cluster3[i].serverID);
                serverinfo->set_hostname(cluster3[i].hostname);
                serverinfo->set_port(cluster3[i].port);
                serverinfo->set_type(cluster3[i].type);
                break;
            }
            ++i;
        }
        break;
    }
    }

    return Status::OK;
  }


  Status GetAllServers(ServerContext* context,const GetAllServersRequset *getAllServersRequset,GetAllServersResponse* getAllServersResponse) override{

    int clusterId = 1;

    while (clusterId <= 3) {
    int i = 0;
    std::vector<zNode>* currentCluster;

    switch (clusterId) {
        case 1:
            currentCluster = &cluster1;
            break;
        case 2:
            currentCluster = &cluster2;
            break;
        case 3:
            currentCluster = &cluster3;
            break;
    }

    // Loop through servers in the current cluster
    while (i < currentCluster->size()) {
        // Create a ServerInfo object and set its parameters based on the current zNode
        ServerInfo server_info;
        set_server_parameters(&server_info, (*currentCluster)[i], clusterId);
        
        // Add the ServerInfo to the response's server list
        getAllServersResponse->add_serverlist()->CopyFrom(server_info);
        ++i;
    }

    ++clusterId;
    }   
   return Status::OK; 
  }

  
  Status Exist(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
  
    int clusterID = serverinfo->clusterid();
    std::vector<zNode>* nodelist = nullptr;

    switch (clusterID) {
    case 1:
        nodelist = &cluster1;
        break;
    case 2:
        nodelist = &cluster2;
        break;
    case 3:
        nodelist = &cluster3;
        break;
    }

    // Check if a valid node list was obtained
    if (nodelist) {
    int i = 0;

    // Iterate through the nodes in the cluster
    while (i < nodelist->size()) {
        zNode node = (*nodelist)[i];

        // Check if the node matches the specified server ID
        if (node.serverID == serverinfo->serverid()) {
            if ((node.isSynchronizer && serverinfo->servertype() == "synchronizer") ||
                (!node.isSynchronizer && serverinfo->servertype() == "server")) {
                
                // Set confirmation status to true and return OK
                confirmation->set_status(true);
                return Status::OK;
            }
        }
        ++i;
    }
    }

    // If no match is found, set confirmation status to false and return OK 
    confirmation->set_status(false);
    return Status::OK;
  }


  Status GetSynchronizer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
    
    int clusterID = id->id();
    std::vector<zNode>* nodelist = nullptr;

    switch (clusterID) {
    case 1:
        nodelist = &cluster1;
        break;
    case 2:
        nodelist = &cluster2;
        break;
    case 3:
        nodelist = &cluster3;
        break;
    }

    // Check if a valid node list was obtained
    if (nodelist) {
    int i = 0;

    // Iterate through the nodes in the cluster
    while (i < nodelist->size()) {

        // Check if the current node is a synchronizer
        if ((*nodelist)[i].isSynchronizer) {

            // Set server information based on the found synchronizer and exit the loop
            set_server_parameters(serverinfo, (*nodelist)[i], clusterID);
            break;
        }
        ++i;
    }
    }   
    return Status::OK;
  }
};



void RunServer(std::string port_no){
  //start thread to check heartbeats
  std::thread hb(checkHeartbeat);
  //localhost = 127.0.0.1
  std::string server_address("127.0.0.1:"+port_no);
  CoordServiceImpl service;
  //grpc::EnableDefaultHealthCheckService(true);
  //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  
  std::string port = "3010";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;
          break;
      default:
	std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunServer(port);
  return 0;
}


// Continuous function to check the heartbeat status of servers
void checkHeartbeat(){
  while(true){
      std::vector<zNode*> servers;

      std::vector<zNode>* clusters[] = {&cluster1, &cluster2, &cluster3};

      int clusterIndex = 0;
      while (clusterIndex < 3) {

        // Check if the current cluster is not empty
        if (clusters[clusterIndex]->size() != 0) {
          int i = 0;

          // Add pointers to servers in the current cluster to the 'servers' vector
          while (i < clusters[clusterIndex]->size()) {
            servers.push_back(&(*clusters[clusterIndex])[i]);
            ++i;
          }
        }
        ++clusterIndex;
      }

      // Loop through each server for heartbeat check
      for(auto& s : servers) {
        if (s->isSynchronizer) {
          continue;
        }

        // Check if the server's heartbeat is overdue
        if (difftime(getTimeNow(),s->last_heartbeat)>10) {
          std::cout<<"Checking cluster id and server id "<<s->clusterID<<":"<<s->serverID<<" is down"<<std::endl;
          s->missed_heartbeat = true;
          s->type="down";
          
          // Reset master status if the server is the master
          if (s->isMaster) {
            s->isMaster = false;
          }
        } else {
          // Update status if the server's heartbeat is within the acceptable range
          s->missed_heartbeat = false;
          s->type="alive";
        }
      }
      
      // Delay for 1 second before the next iteration
      sleep(1);
    }
} 
