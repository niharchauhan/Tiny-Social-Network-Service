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
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include <iostream>
#include <sstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "synchronizer.grpc.pb.h"
#include "coordinator.grpc.pb.h"

namespace fs = std::filesystem;

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
using csce438::ID;
using csce438::SynchService;
using grpc::Channel;

using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::ClientContext;

using csce438::CoordService;
using grpc::Channel;
using grpc::ClientContext;
using csce438::Confirmation;
using csce438::ServerInfo;
using csce438::GetSlaveRequset;

using csce438::GetAllServersRequset;
using csce438::GetAllServersResponse;

using csce438::GetAllUsersRequest;
using csce438::AllUsers;

using csce438::GetFLRequest;
using csce438::GetFLResponse;

using csce438::GetTLRequest;
using csce438::GetTLResponse;

using csce438::ID;

using csce438::ResynchServerRequest;
using csce438::ResynchServerResponse;

int synchID = 1;
std::vector<std::string> get_lines_from_file(std::string);
void run_synchronizer(std::string,std::string,std::string,int);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);

void sync_all_user(std::shared_ptr<CoordService::Stub> coord_stub_,int synchID);
void sync_follow(std::shared_ptr<CoordService::Stub> coord_stub_,int synchID);
void sync_timeline(std::shared_ptr<CoordService::Stub> coord_stub_,int synchID);

std::string getAllUserFileName(std::string clusterID,std::string serverID);
std::string getFollowFileName(std::string clusterID,std::string serverID);
std::string getTimelineFileName(std::string clusterID,std::string serverID,std::string uid);

int lockFile(const std::string& filePath);
int unlockFile(const std::string& filePath);

class SynchServiceImpl final : public SynchService::Service {
  public:
    int SynchID;
    Status GetAllUsers(ServerContext* context, const GetAllUsersRequest* getAllUsersRequest, AllUsers* allusers) override{
        std::vector<std::string> list = get_all_users_func(synchID);

        //package list
        for(auto s:list){
            //std::cout<<"User: "<<s<<std::endl;
            allusers->add_allusers(s);
        }

        //return list
        return Status::OK;
    }

    Status GetFL(ServerContext* context, const GetFLRequest* getFLRequest,GetFLResponse* getFLResponse) override{
        
        std::vector<std::string> fl = get_tl_or_fl(synchID, -1,false);

        //now populate TLFL tl and fl for return
        auto s = fl.begin();
        while (s != fl.end()) {
          getFLResponse->add_lines(*s);
          ++s;
        }
        return Status::OK;
    }

     Status GetTL(ServerContext* context, const GetTLRequest* getTLRequest,GetTLResponse* getTLResponse) override {

        std::vector<std::string> fl = get_tl_or_fl(synchID, getTLRequest->uid(),true);

        //now populate TLFL tl and fl for return
        auto it = fl.begin();
        while (it != fl.end()) {
          getTLResponse->add_lines(*it);
          ++it;
        }

        return Status::OK;
    }

    bool duplicateDocument(const std::string& sourceFileName, const std::string& destinationFileName) {
      std::ifstream sourceFile(sourceFileName, std::ios::binary);
      if (sourceFile.is_open() == false) {
          std::cerr << "Unable to open source file: " << sourceFileName << std::endl;
          return false;
      }

      std::ofstream destinationFile(destinationFileName, std::ios::binary | std::ios::trunc);
      if (destinationFile.is_open() == false) {
          std::cerr << "Unable to open destination file: " << sourceFileName << std::endl;
          sourceFile.close();
          return false;
      }

      destinationFile << sourceFile.rdbuf();

      sourceFile.close();
      destinationFile.close();

      return true;
    }

    Status ResynchServer(ServerContext* context, const ResynchServerRequest* resynchServerRequest, ResynchServerResponse* resynchServerResponse){
        std::string backupServerType;
        
        std::cout<<"Resync server started!"<<std::endl;

        std::string slaveID = std::to_string(resynchServerRequest->serverid());

        std::string masterID;
        masterID = (slaveID == "1") ? "2" : "1";

        std::string syncID = std::to_string(SynchID);
        std::string clusterID=syncID;

        std::string MasterUserFilename = getAllUserFileName(clusterID,masterID);
        std::string SlaveUserFilename = getAllUserFileName(clusterID,slaveID);
        std::cout<<"  > User File Synchronization, "<<MasterUserFilename << " - " << SlaveUserFilename<<std::endl;
        duplicateDocument(MasterUserFilename,SlaveUserFilename);

        std::string MasterFollowFilename = getFollowFileName(clusterID,masterID);
        std::string SlaveFollowFilename = getFollowFileName(clusterID,slaveID);
        std::cout<<"  > Follow Relationship Synchronization, "<<MasterFollowFilename << " - " << SlaveFollowFilename<<std::endl;
        duplicateDocument(MasterFollowFilename,SlaveFollowFilename);

        std::vector<std::string> allUsers = get_all_users_func(SynchID);
        for(int i=0;i<allUsers.size();i++){
          std::string uid = allUsers[i];

          std::string MasterTlFilename = getTimelineFileName(clusterID,masterID,uid);
          std::string SlaveTlFilename = getTimelineFileName(clusterID,slaveID,uid);
          std::cout<<"  > Timeline File Synchronization, "<<MasterTlFilename << " - " << SlaveTlFilename<<std::endl;
          duplicateDocument(MasterTlFilename,SlaveTlFilename);
        }

        return Status::OK;
    }
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID){
  //localhost = 127.0.0.1
  std::string server_address("127.0.0.1:"+port_no);
  std::cout<<"Start, "<<server_address<<std::endl;
  SynchServiceImpl service;
  service.SynchID = synchID;

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

  std::thread t1(run_synchronizer,coordIP, coordPort, port_no, synchID);
  /*
  TODO List:
    -Implement service calls
    -Set up initial single heartbeat to coordinator
    -Set up thread to run synchronizer algorithm
  */

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}



int main(int argc, char** argv) {
  
  int opt = 0;
  std::string coordIP="127.0.0.1";
  std::string coordPort="3010";
  std::string port = "3029";

  while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1){
    switch(opt) {
      case 'h':
          coordIP = optarg;
          break;
      case 'k':
          coordPort = optarg;
          break;
      case 'p':
          port = optarg;
          break;
      case 'i':
          synchID = std::stoi(optarg);
          break;
      default:
	         std::cerr << "Invalid Command Line Argument\n";
    }
  }

  RunServer(coordIP, coordPort, port, synchID);
  return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID){

    std::string login_info(coordIP + ":" + coordPort);
    grpc::ChannelArguments channel_args;
    std::shared_ptr<Channel> channel = grpc::CreateCustomChannel(login_info, grpc::InsecureChannelCredentials(), channel_args);
    std::shared_ptr<CoordService::Stub> coord_stub_ = CoordService::NewStub(channel);
    std::cout<<"MADE STUB"<<std::endl;

    ServerInfo msg;
    Confirmation c;
    grpc::ClientContext context;

    msg.set_clusterid(synchID);
    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1");
    msg.set_port(port);
    msg.set_servertype("synchronizer");
    coord_stub_->Create(&context,msg,&c);

    //send init heartbeat

    //TODO: begin synchronization process
    while(true){
        //change this to 30 eventually
        sleep(10);

        std::cout<<"\n\nSync Started!"<<std::endl;

        std::cout<<"sync_all_user function called"<<std::endl;
        sync_all_user(coord_stub_,synchID);
          
        std::cout<<"sync_follow function called"<<std::endl;
        sync_follow(coord_stub_,synchID);

        std::cout<<"sync_timeline function called"<<std::endl;
        sync_timeline(coord_stub_,synchID);

        std::cout<<"Sync Completed!\n"<<std::endl;
	   
    }
    return;
}

std::shared_ptr<SynchService::Stub> get_sync_stub(std::string host,std::string port){
  std::string login_info(host + ":" + port);

  grpc::ChannelArguments channel_args;
  std::shared_ptr<Channel> slave_channel = grpc::CreateCustomChannel(login_info, grpc::InsecureChannelCredentials(), channel_args);
  std::shared_ptr<SynchService::Stub> stub_ = SynchService::NewStub(slave_channel);

  return stub_;
}

std::string getAllUserFileName(std::string clusterID,std::string serverID){
  std::stringstream ss;
    ss << "USER_" << clusterID << "_" << serverID << ".txt";
    return ss.str();
}

std::string getFollowFileName(std::string clusterID,std::string serverID){
    std::stringstream ss;
    ss << "FOLLOWER_" << clusterID << "_" << serverID << ".txt";
    return ss.str();
}

std::string getTimelineFileName(std::string clusterID,std::string serverID,std::string uid){
    std::stringstream ss;
    ss << "TIMELINE_" << clusterID << "_" << serverID << "_" << uid << ".txt";
    std::string file = ss.str();
    return file;
}

void writeSetToFile(const std::set<std::string>& mySet, const std::string& filename) {
    std::ofstream file(filename, std::ios::out | std::ios::trunc);

    if (file.is_open() == false) {
        throw std::runtime_error("Failed to open the file: " + filename);
    }
    
    auto it = mySet.begin();
    while (it != mySet.end()) {
      file << *it << std::endl;
      ++it;
    }

    file.close();
}

void writeListToFile(std::vector<std::string> list, const std::string& filename) {
    std::ofstream file(filename, std::ios::out | std::ios::trunc);

    if (file.is_open() == false) {
        throw std::runtime_error("Failed to open the file: " + filename);
    }
    
    auto it = list.begin();
    while (it != list.end()) {
      file << *it << std::endl;
      ++it;
    }

    file.close();
}

// Synchronize users from other clusters.
void sync_all_user(std::shared_ptr<CoordService::Stub> coord_stub_,int synchID){
  
  ClientContext *context = new ClientContext(); 
  GetAllServersRequset *req = new GetAllServersRequset();
  GetAllServersResponse *reply = new GetAllServersResponse();
  coord_stub_->GetAllServers(context,*req,reply);

  std::set<std::string> userSet;

  int i = 0;
while (i < reply->serverlist_size()) {
    const ServerInfo& server_info = reply->serverlist(i);

    if(server_info.servertype()!="synchronizer") {
        ++i;
        continue; // retrieve user information from the synchronizer
    }
    
    if(server_info.serverid() == synchID) {
        ++i;
        continue; // avoid sending a request to itself
    }

    std::shared_ptr<SynchService::Stub> stub = get_sync_stub(
        server_info.hostname(),
        server_info.port()
    );

    grpc::ClientContext context;
    GetAllUsersRequest req;
    AllUsers reponse;
    stub->GetAllUsers(&context,req,&reponse);

    int j = 0;
    while (j < reponse.allusers_size()) {
        const std::string& user = reponse.allusers(j);
        userSet.insert(user);
        ++j;
    }

    ++i;
}


  // Include users from my pair
  std::vector<std::string> list = get_all_users_func(synchID);
  // Insert users into the set
  while (!list.empty()) {
    userSet.insert(list.back());
    list.pop_back();
  }


  std::cout<<"User List: "<<std::endl;
  // Print elements in the set
  auto it = userSet.begin();
  while (it != userSet.end()) {
    std::cout << *it << std::endl;
    ++it;
  }

  // write users to user file
  std::string master_users_file = getAllUserFileName(std::to_string(synchID),"1");
  std::string slave_users_file = getAllUserFileName(std::to_string(synchID),"2");
  writeSetToFile(userSet,master_users_file);
  writeSetToFile(userSet,slave_users_file);
}

void eliminateDuplicates(std::vector<std::string>& vec) {
    std::set<std::pair<int, int>> observed;
    std::vector<std::string> distinctVector;

    while (!vec.empty()) {
      const auto& line = vec.back();
      int first, second;
      if (sscanf(line.c_str(), "%d %d", &first, &second) == 2) {
        if (observed.insert({first, second}).second) {
            distinctVector.push_back(line);
        }
      }
      vec.pop_back();
    }

    vec.swap(distinctVector);
}

void collectRelations(int synchID,const GetFLResponse& response, std::vector<std::string>& aggregatedRelations) {
    std::vector<std::string> users = get_all_users_func(synchID);
    std::unordered_set<std::string> currentClusterUsers(users.begin(),users.end());

    int i = 0;
    while (i < response.lines_size()) {
    
      std::istringstream iss(response.lines(i));
      std::string user1, user2, time;
      if (iss >> user1 >> user2 >> time) {
        // Add to the aggregated relationships only when both users are in the current cluster
        if (currentClusterUsers.count(user2) > 0) {
            aggregatedRelations.push_back(response.lines(i));
        }
      }
      ++i;
    }

    eliminateDuplicates(aggregatedRelations);
}

/*
  Function for sync all follow relationships
*/
void sync_follow(std::shared_ptr<CoordService::Stub> coord_stub_,int synchID){
  ClientContext *context = new ClientContext(); 
  GetAllServersRequset *req = new GetAllServersRequset();
  GetAllServersResponse *reply = new GetAllServersResponse();
  coord_stub_->GetAllServers(context,*req,reply);

  std::string masterf = getFollowFileName(std::to_string(synchID),"1");
  std::string slavef = getFollowFileName(std::to_string(synchID),"2");

   // Acquire lock
    std::cout<<" > Attempt to acquire lock: "<<masterf<<" "<<slavef<<std::endl;

    for (;;) {
      int k = lockFile(masterf);
      if (k == 0) break;
    }

    for (;;) {
      int m = lockFile(slavef);
      if (m == 0) break;
    }

    std::cout<<" > Acquire lock: "<<masterf<<" "<<slavef<<std::endl;

  // cluster-specific follow relationships
  std::vector<std::string> masterR = get_lines_from_file(masterf);
  std::vector<std::string> slaveR = get_lines_from_file(slavef);
  std::vector<std::string> aggregatedRelations;

  // Choose the larger set of follow relationships
  aggregatedRelations = (masterR.size() > slaveR.size()) ? masterR : slaveR;

  std::cout<<"Previous Follow Relationships:"<<std::endl;

  int j = 0;
  while (j < aggregatedRelations.size()) {
    std::cout<<aggregatedRelations[j]<<std::endl;
    j++;
  }

  for (int i = 0; i < reply->serverlist_size(); ++i) {
    const ServerInfo& server_info = reply->serverlist(i);

    if(server_info.servertype()!="synchronizer") continue; // retrieve user information from the synchronizer
    if(server_info.serverid() == synchID) continue; // avoid send request to itself

    std::shared_ptr<SynchService::Stub> stub = get_sync_stub(server_info.hostname(), server_info.port());

    // Retrieve follow relationships
    grpc::ClientContext context;
    GetFLRequest req;
    GetFLResponse reponse;
    stub->GetFL(&context,req,&reponse);

    collectRelations(synchID,reponse,aggregatedRelations);

    unlockFile(masterf);
    unlockFile(slavef);
  }

  std::cout<<"User Connections:"<<std::endl;

  int q = 0;
  while (q < aggregatedRelations.size()) {
    std::cout<<aggregatedRelations[q]<<std::endl;
    q++;
  }

  writeListToFile(aggregatedRelations,masterf);
  writeListToFile(aggregatedRelations,slavef);
}

std::set<int> getFollowedUsers(int uid,std::string clusterID) {
    std::set<int> followedUsers;

    std::string documentName = getFollowFileName(clusterID, "1");

    std::ifstream file(documentName);
    std::string user1, user2, time;

    if (file.is_open() == false) {
        std::cerr <<"Unable to open the file!" << std::endl;
        return followedUsers;
    }

    for (; file >> user1 >> user2 >> time;) {
      if (stoi(user1) == uid) {
        followedUsers.insert(stoi(user2));
      }
    }

    file.close();
    return followedUsers;
}


struct TimelineEntry {
    std::string timestamp;
    std::string text;
    int id;

    bool operator<(const TimelineEntry& other) const {
        return timestamp > other.timestamp;
    }
};

TimelineEntry parseTimelineEntry(const std::string& entryStr) {
    // Create an input string stream from the provided entry string
    std::istringstream iss(entryStr);

    // Create a TimelineEntry to store the parsed values
    TimelineEntry entry;

    // Parse the values from the entry string and assign them to the TimelineEntry
    iss >> entry.id >> entry.text >> entry.timestamp;
    return entry;
}

int lockFile(const std::string& filePath) {
    return 0;
}

int unlockFile(const std::string& filePath) {
    return 0;
}

void processAndWriteTimeline(
    const std::vector<std::string>& listEntries,
    int following_userID,
    int synchID,
    int masterID,
    int uid
  ) {

    int slaveID = (masterID == 1) ? 2 : 1;

    std::set<TimelineEntry> entries;
    std::string fileName = getTimelineFileName(std::to_string(synchID), std::to_string(masterID), std::to_string(uid));
    std::string fileNameSlave = getTimelineFileName(std::to_string(synchID), std::to_string(slaveID), std::to_string(uid));

    // Acquire lock
    std::cout<<" > Attempting to acquire lock: "<<fileName<<" "<<fileNameSlave<<std::endl;
    for (;;) {
      int k = lockFile(fileName);
      if (k == 0) break;
    }

    for (;;) {
      int k = lockFile(fileNameSlave);
      if (k == 0) break;
    }

    std::cout<<" > Acquire lock: "<<fileName<<" "<<fileNameSlave<<std::endl;
    
    std::ifstream fileIn(fileName);
    TimelineEntry entry;

    if (fileIn.is_open() == true) {
        for (std::string line; std::getline(fileIn, line);) {
          entries.insert(parseTimelineEntry(line));
        }
        fileIn.close();
    } else {
        std::cerr << "Unable to open the file for reading!" << std::endl;
    }

    for (const auto& listEntryStr : listEntries) {
        // Exclude this entry if it is not published by someone being followed.
        TimelineEntry entry = parseTimelineEntry(listEntryStr);
        if(entry.id != following_userID) continue;

        entries.insert(parseTimelineEntry(listEntryStr));
    }

    std::cout<<"DOCUMENTNAME 1: "<<" : "<<fileName<<std::endl;
    std::ofstream fileOut(fileName);
    if (fileOut.is_open() == true) {
        for (const auto& e : entries) {
            std::cout<<"W: "<<" : "<<e.id << " " << e.text << " " << e.timestamp<<std::endl;
            fileOut << e.id << " " << e.text << " " << e.timestamp << std::endl;
        }
        fileOut.close();
    } else {
        std::cerr << "Unable to write to file" << std::endl;
    }
    
    std::cout<<"DOCUMENTNAME 2: "<<" : "<<fileNameSlave<<std::endl;
    std::ofstream fileOutSlave(fileNameSlave);
    if (fileOutSlave.is_open() == true) {
        for (const auto& e : entries) {
            std::cout<<"W: "<<" : "<<e.id << " " << e.text << " " << e.timestamp<<std::endl;
            fileOutSlave << e.id << " " << e.text << " " << e.timestamp << std::endl;
        }
        fileOutSlave.close();
    } else {
        std::cerr << "Unable to write to file" << std::endl;
    }

    unlockFile(fileName);
    unlockFile(fileNameSlave);
}

// Synchronization Timeline Function
void sync_timeline(std::shared_ptr<CoordService::Stub> coord_stub_,int synchID) {
  // Get the list of servers from the coordinator.
  ClientContext *context = new ClientContext(); 
  GetAllServersRequset *req = new GetAllServersRequset();
  GetAllServersResponse *reply = new GetAllServersResponse();
  coord_stub_->GetAllServers(context,*req,reply);

  // Get information about the master server.
  context = new ClientContext(); 
  ID *id = new ID();
  id->set_id(synchID);
  ServerInfo *master = new ServerInfo();
  coord_stub_->GetServer(context,*id,master);
  
  int masterID = master->serverid();
  std::cout<<"Current Master is: "<<masterID<<std::endl;

  // Get the list of all users in the current cluster.
  std::vector<std::string> allUser = get_all_users_func(synchID);

  for (int i=0; i<allUser.size(); i++){
    std::string user=allUser[i];
    int uid = stoi(user);
    int cid = (uid-1)%3 + 1;

    if(cid != synchID) continue;
    std::cout<<"Synchronization timeline for user: "<<uid<<std::endl;

    // Get the set of users followed by the current user.
    std::set<int> followedUsers = getFollowedUsers(uid, std::to_string(synchID));
    for (int following : followedUsers) {
      ServerInfo info;
      int targetClusterId = (following-1)%3+1;

      std::cout<<"Synchronization timeline from user: "<<targetClusterId<<std::endl;

      // Find the synchronizer server in the target cluster.
      for (int j = 0; j < reply->serverlist_size(); ++j) {
        const ServerInfo& server_info = reply->serverlist(j);
        if(server_info.servertype()!="synchronizer") continue; // retrieve user information from the synchronizer
        if(server_info.clusterid() == targetClusterId){
          info = server_info;
          break;
        }
      }

      std::cout<<"Create a stub using: "<<targetClusterId<<"  "<<info.hostname()+":"+info.port()<<std::endl;
      std::shared_ptr<SynchService::Stub> stub = get_sync_stub(info.hostname(), info.port());

      grpc::ClientContext context;
      GetTLRequest req;
      req.set_uid(following);

      GetTLResponse response;
      stub->GetTL(&context,req,&response);

      std::cout<<"Retrieve timeline from: "<<following<<std::endl;
      std::vector<std::string> listEntries;
      for (int j = 0; j < response.lines_size(); ++j) {
        std::string line = response.lines(j);
        std::cout<<j<<" : "<<line<<std::endl;
        listEntries.push_back(line);
      }
      std::cout<<"processAndWriteTimeline"<<std::endl;
      processAndWriteTimeline(listEntries,following,synchID,masterID,uid);
    }
  }
}

// Function to read lines from a file and store them in a vector
std::vector<std::string> get_lines_from_file(std::string filename){
  std::vector<std::string> users;
  std::string user;
  std::ifstream file; 
  file.open(filename);

  // Check if the file is empty
  if(file.peek() == std::ifstream::traits_type::eof()) {
    file.close();
    return users;
  }

  // Read lines from the file using a while loop
  while(file){
    getline(file,user);

    if(user.empty() == false) {
      users.push_back(user);
    }
  }

  file.close();

  return users;
}

// Function to check if a file contains a specific user
bool file_contains_user(std::string filename, std::string user){
    std::vector<std::string> users;
    users = get_lines_from_file(filename);
    int i = 0;

    // Loop through the vector to check if the user is present
    while (i < users.size()) {
      if(user == users[i]){
        // Return true if the user is found
        return true;
      }
      i++;
    }
    // Return false if the user is not found
    return false;
}

std::vector<std::string> get_all_users_func(int synchID){
    std::string master_users_file = getAllUserFileName(std::to_string(synchID),"1");
    std::string slave_users_file = getAllUserFileName(std::to_string(synchID),"2");

    std::vector<std::string> master_user_list = get_lines_from_file(master_users_file);
    std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file);

    if(master_user_list.size() >= slave_user_list.size())
        return master_user_list;
    else
        return slave_user_list;
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID,bool tl) {
    std::string slave_fn = "";
    std::string master_fn = "";
    
    if (!tl) {
        master_fn = getFollowFileName(std::to_string(synchID),"1");
        slave_fn = getFollowFileName(std::to_string(synchID),"2");
    } else {
        master_fn = getTimelineFileName(std::to_string(synchID),"1",std::to_string(clientID));
        slave_fn = getTimelineFileName(std::to_string(synchID),"2",std::to_string(clientID));
    }

    std::vector<std::string> m = get_lines_from_file(master_fn);
    std::vector<std::string> s = get_lines_from_file(slave_fn);

    if (m.size() >= s.size()) {
        return m;
    }else{
        return s;
    }
}