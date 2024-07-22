/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <thread>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#include <sys/inotify.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <sstream>

#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"
#include "synchronizer.grpc.pb.h"


using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;

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
using csce438::ID;

using csce438::SynchService;
using csce438::ResynchServerRequest;
using csce438::ResynchServerResponse;

struct Client {
  std::string username;

  bool connected = true;

  int idx;

  std::vector<Client*> client_followers;
  std::vector<std::string> client_followers_time;

  std::vector<Client*> client_following;
  std::vector<std::string> client_following_time;

  ServerReaderWriter<Message, Message>* stream = 0;

  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

// A container that holds references to all the clients that have been instantiated.
std::vector<Client> client_db;

int getVersionOfUser(std::string uname){

  // Iterate through the client database to find the user
  for(Client c:client_db){
    if(c.username == uname){
      // Return the version (index) of the user
      return c.idx;
    }
  }
  return -1;
}

int lockFile(const std::string& filePath) {
    return 0;
}

int unlockFile(const std::string& filePath) {
    return 0;
}

std::vector<std::string> readFileLines(const std::string& filePath) {
  // Attempt to acquire a lock on the file
    for (;;){
      std::cout << "Attempting to acquire lock\n";
      int k = lockFile(filePath);
      if (k == 0) {
        break;
      }
    }

    std::cout<<"Acquire lock,"<<filePath<<std::endl;

    std::vector<std::string> lines;
    std::string line;
    std::ifstream file(filePath);

    while (getline(file, line)) {
        lines.push_back(line);
    }

    unlockFile(filePath);
    return lines;
}

std::vector<std::string> analyzeFiles(const std::vector<std::string>& oldLines, const std::vector<std::string>& newLines) {
    std::vector<std::string> addedLines;

    // Get sizes of old and new sets
    size_t oldSize = oldLines.size();
    size_t newSize = newLines.size();

    int linenum = newSize-oldSize;

    // Iterate through the newly added lines and add them to the vector
    int i = linenum-1;
    while (i >= 0) {
        addedLines.push_back(newLines[i]);
        --i;
    }

    return addedLines;
}


std::vector<std::string> identifyFileModifications(const std::string& filePath, std::vector<std::string>* oldLines) {

    std::vector<std::string> addedLines;

    // Retrieve the current state of the file
    std::vector<std::string> newLines = readFileLines(filePath);

    // Analyze changes between the old and new states
    addedLines = analyzeFiles(*oldLines, newLines);

    // Update the oldLines vector with the current state
    oldLines->clear();
    oldLines->resize(newLines.size());
    std::copy(newLines.begin(), newLines.end(), oldLines->begin());
    return addedLines;
}

void deliverMessage(std::string line,ServerReaderWriter<Message, Message>* stream,std::string uname) {
  std::istringstream iss(line);
  std::vector<std::string> tokens;
  std::string token;
  
  while (iss >> token) {
    tokens.push_back(token);
  }

  // Constructing a new message
  Message m;
  m.set_username(tokens[0]);
  m.set_msg(tokens[1]);

  std::cout<<m.username()<<" deliver message," <<uname<<std::endl;

  if(m.username() == uname) {
    return;
  }

  google::protobuf::Timestamp *timestamp = new google::protobuf::Timestamp();
  if (google::protobuf::util::TimeUtil::FromString(tokens[2], timestamp)) {
      std::cout << "Timestamp: " << timestamp->DebugString() << std::endl;
  } else {
      std::cerr << "FAIL!" << std::endl;
  }

  m.set_allocated_timestamp(timestamp);

  // deliver message to user
  stream->Write(m);   
}

void monitorFile(std::string filepath,ServerReaderWriter<Message, Message>* stream,std::string uname,int uv){

  std::vector<std::string> init = readFileLines(filepath);
  std::vector<std::string>* oldLines  = &init;

    for (;;) {
      std::this_thread::sleep_for(std::chrono::milliseconds(4000));
      
      // Determine the version of user
      if(getVersionOfUser(uname)!=uv){
        break;
      }

      std::vector<std::string> differences = identifyFileModifications(filepath,oldLines);
      if (differences.empty() == false) {
        for (const auto& line : differences) {
          std::cout << "Inserted a latest line: " << line << std::endl;
          deliverMessage(line,stream,uname);
        }
      }
    }

    std::cout<<"User: "<<uname<<" has logged in again"<<std::endl;
}

class SNSServiceImpl final : public SNSService::Service {
public:
  std::string clusterID;
  std::string serverID;
  std::string coordinatorIP;
  std::string coordinatorPort;

  std::shared_ptr<CoordService::Stub> stub_;
  
  std::string getTimelineFileName(std::string username){
    std::stringstream ss;
    ss << "TIMELINE_" << clusterID << "_" << serverID << "_" << username << ".txt";
    return ss.str();
  }
  std::string getFollowFileName(){
    std::stringstream ss;
    ss << "FOLLOWER_" << clusterID << "_" << serverID << ".txt";
    return ss.str();
  }
  std::string getAllUserFileName(){
    std::stringstream ss;
    ss << "USER_" << clusterID << "_" << serverID << ".txt";
    return ss.str();
  }

  std::shared_ptr<SNSService::Stub>  get_slave_stub() {
    ClientContext *context = new ClientContext(); 

    // Create a request object for getting slave information
    GetSlaveRequset *req = new GetSlaveRequset();
    req->set_clusterid(atoi(clusterID.c_str()));

    ServerInfo *reply = new ServerInfo();

    // Call the GetSlave RPC to retrieve server information
    stub_->GetSlave(context,*req,reply);

    // Check if the returned server ID matches the current server's ID
    if(std::to_string(reply->serverid()) == serverID) {
      std::cout<<"Slave server called!"<<std::endl;
      return std::shared_ptr<SNSService::Stub>();
    }

    // Check if the returned server type is not "alive"
    if (reply->type()!="alive"){
      std::cout<<"Slave server is not operational "<<reply->serverid()<<", having host: "<<reply->hostname()<<", and port: "<<reply->port()<<std::endl;
      return std::shared_ptr<SNSService::Stub>();
    }

    std::cout<<"Slave server details: "<<reply->serverid()<<", Host: "<<reply->hostname()<<", Port: "<<reply->port()<<std::endl;
    std::string host = reply->hostname();
    std::string port = reply->port();

    std::string login_info(host + ":" + port);

    // Create a custom gRPC channel with insecure credentials
    grpc::ChannelArguments channel_args;
    std::shared_ptr<Channel> slave_channel = grpc::CreateCustomChannel(login_info, grpc::InsecureChannelCredentials(), channel_args);
    
    // Create a new stub using the custom channel
    std::shared_ptr<SNSService::Stub> slave_stub_ = SNSService::NewStub(slave_channel);

    return slave_stub_;
  }

  void persistAllClients(){
    std::ofstream file(getAllUserFileName());
    
    if (file.is_open() == false) {
        std::cerr <<"Unable to open the file"<< std::endl;
        return;
    }

    int i = 0;
    while (i < client_db.size()) {
      file << client_db[i].username << std::endl;
      i++;
    }
  }

  void fetchAllClients(){
    std::ifstream file(
      getAllUserFileName()
    );
    if (file.is_open() == false) {
        std::ofstream file_create(getAllUserFileName(), std::ios::app);
        file_create.close();
        file.open(getAllUserFileName());
    }

    if (file.is_open() == false) {
        throw std::runtime_error("Error in opening file: " + getAllUserFileName());
    }

    std::vector<std::string> elements;
    std::string line;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string element;
        if (iss >> element) {
          elements.push_back(element);
          
        }
    }

    for(int i=elements.size()-1;i>=0;i--){
      bool find=false;
      for (Client c: client_db) {
        if(c.username == elements[i]){
          find=true;
          break;
        }
      }

      if (find == true) {
        continue;
      }

      std::cout<<"Unable to find the user: "<<elements[i]<<", new user"<<std::endl;

      Client *user=new Client();
      user->username = elements[i];
      client_db.push_back(*user);
    }
  }

  void retrieveFollowerAndFollowingFromDocument() {
    std::ifstream file(getFollowFileName());

    for (;;) {
      // Try to lock the file; elem will be 0 if successful, break otherwise
      int elem = lockFile(getFollowFileName());
      if (elem==0) { 
        break;
      }
    }

    std::string time, client1ID, client2ID;

    std::unordered_map<std::string, Client*> userMapping;
    
    // Populate userMapping with clients from the client_db, clearing previous follower and following lists
    for (auto& client : client_db) {
        userMapping[client.username] = &client;
        client.client_followers.clear();
        client.client_following.clear();

        client.client_followers_time.clear();
        client.client_following_time.clear();
    }

    // Read follower and following information from the file
    while (file >> client1ID >> client2ID >> time) {
        // Check if both clients exist in the userMapping
        if (userMapping.find(client1ID) != userMapping.end() && userMapping.find(client2ID) != userMapping.end()) {
            // Retrieve pointers to the follower and following clients
            Client* follower = userMapping[client1ID];
            Client* following = userMapping[client2ID];

            // Update follower's following list and time
            follower->client_following.push_back(following);
            follower->client_following_time.push_back(time);

            // Update following's followers list and time
            following->client_followers.push_back(follower);
            following->client_followers_time.push_back(time);
        }
    }

    file.close();
    unlockFile(getFollowFileName());
  }

  void persistFollowerAndFollowingToDocument() {
    std::ofstream file(getFollowFileName());

    std::unordered_set<std::string> persistedEntries;

    for (const auto& client : client_db) {
      int position=0;
        for (const auto* following : client.client_following) {
            // std::string entry = client.username + " " + following->username+" "+client.client_following_time[position];
            std::ostringstream entryStream;
            entryStream << client.username << " " << following->username << " " << client.client_following_time[position];
            std::string entry = entryStream.str();
            
            if (persistedEntries.find(entry) == persistedEntries.end()) {
                file << entry << std::endl;
                persistedEntries.insert(entry);
            }
            position++;
        }
    }

    file.close();
  }

  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {

    std::cout<<"LIST"<<std::endl;

    // Fetch all clients and retrieve follower and following information from the document
    fetchAllClients();
    retrieveFollowerAndFollowingFromDocument();

    // Populate list_reply with usernames of all clients in the database
    for(int i=0;i<client_db.size();i++){
      list_reply->add_all_users(client_db[i].username);
    }

    // Find the specified user in the client database
    for(int i=0;i<client_db.size();i++){
      if(client_db[i].username != request->username()) continue;

      // Iterate through the client's followers and add their usernames to list_reply
      for(int j=0;j<client_db[i].client_followers.size();j++){
        list_reply->add_followers(client_db[i].client_followers[j]->username);
      }
    }

    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {

    // Fetch all clients and retrieve follower and following information from the document
    fetchAllClients();
    retrieveFollowerAndFollowingFromDocument();

    std::cout<<"Follow: "<<request->username()<<" -> "<<request->arguments()[0]<<std::endl;

    // Check if the user is trying to follow themselves
    if(request->username() == request->arguments()[0]){
      reply->set_msg("Can't follow self");
      return Status::OK; 
    }

    Client *user,*target;
    for(int i=0;i<client_db.size();i++){
      if(client_db[i].username == request->username()){
        user = &client_db[i];
      }
    }
    bool elem = true;
    for (int i=0; i<client_db.size(); i++) {
      if (client_db[i].username == request->arguments()[0]) {
        target = &client_db[i];
        elem=false;
        break;
      }
    }

    // If the target user is not found, log and set an error message in the reply
    if(elem == true){
      std::cout<<"Unable to find "<<request->arguments()[0]<<std::endl;
      reply->set_msg("NO_TARGET");
      return Status::OK;
    }

    for(int i=0;i<user->client_following.size();i++){
      if(user->client_following[i]->username == request->arguments()[0]){
        reply->set_msg("RE-FOLLOW");
        return Status::OK;
      }
    }

    // Add target to user's following list and user to target's followers list
    user->client_following.insert(user->client_following.begin(),target);
    target->client_followers.insert(target->client_followers.begin(),user);

    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
    auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(duration - seconds);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(seconds.count());
    timestamp->set_nanos(nanos.count());
    std::string t = google::protobuf::util::TimeUtil::ToString(*timestamp);
    
    // Insert the timestamp into user and target's timestamp lists
    user->client_following_time.insert(user->client_following_time.begin(),t);
    target->client_followers_time.insert(target->client_followers_time.begin(),t);

    // Persist follower and following information to the document
    persistFollowerAndFollowingToDocument();

    // Perform follow operation with the slave server
    followWithSlaveServer(request);

    reply->set_msg("OK");
    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {

    std::cout<<"UnFollow:"<<request->username()<<" -> "<<request->arguments()[0]<<std::endl;

    Client *user,*target;

    for(int i=0;i<client_db.size();i++){
      if(client_db[i].username == request->username()){
        user = &client_db[i];
      }
    }
    bool elem=true;

    // Find the target user in the client database
    for(int i=0;i<client_db.size();i++){
      if(client_db[i].username == request->arguments()[0]){
        target = &client_db[i];
        elem=false;
        break;
      }
    }

    // If the target user is not found, log and set an error message in the reply
    if(elem == true){
      std::cout<<"Unable to find "<<request->arguments()[0]<<std::endl;
      reply->set_msg("NO_TARGET");
      return Status::OK;
    }

    int position=0;
    elem=true;

    // Iterate through user's following list to find and remove the target
    for (Client* u: user->client_following) {
        if(u->username == request->arguments()[0]){
          user->client_following.erase(user->client_following.begin()+position);
          elem=false;
          break;
        }
        position++;
    }

    position=0;

    // Iterate through target's followers list to find and remove the user
    for (Client* u: target->client_followers) {
        if(u->username == request->username()){
          target->client_followers.erase(target->client_followers.begin()+position);
          break;
        }
        position++;
    }

    // Perform unfollow operation with the slave server
    unfollowWithSlaveServer(request);

    reply->set_msg("OK");
    return Status::OK; 
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    // Fetch all clients from the database
    fetchAllClients();

    std::cout<<"Login:"<<request->username()<<std::endl;

    bool isUserPresent=false;

    // Iterate through the client database
    for(int i=0;i<client_db.size();i++) {
      // Check if the user is already connected
      if(client_db[i].username == request->username() && client_db[i].connected==true){
        isUserPresent = true;
        client_db[i].stream = nullptr;
        client_db[i].idx++;
      }
      // Update the user's connection status to connected, reset the stream, set success message, and increment login index
      else if(client_db[i].username == request->username() && client_db[i].connected==false){
        client_db[i].connected==true;
        client_db[i].stream = nullptr;
        reply->set_msg("OK");
        isUserPresent =true;
        client_db[i].idx++;
      }
    }

    // If the user is not present, create a new client and insert it into the database
    if (isUserPresent == false) {
      Client *newClient = new Client();
      newClient->username = request->username();
      client_db.insert(client_db.begin(),*newClient);
    }
   
    // Generate the timeline file path
    std::string timelineFile = getTimelineFileName(request->username());
    
    // Check if the timeline file exists; if not, generate a new file
    if (documentPresent(timelineFile) == false) {
      std::cout<<"Generate a new file"<<std::endl;

      // Open the timeline file for writing
      std::ofstream timeline_file(timelineFile);
      if (!timeline_file.is_open()) {
          std::cerr << "Failed to open file" << std::endl;
          reply->set_msg("Deny");
          return Status::OK;
      }
      timeline_file.close();
    }
  
    // Persist the updated client database
    persistAllClients();

    // Perform login operation with the slave server
    loginWithSlaveServer(request);

    reply->set_msg("OK");
    return Status::OK;
  }

  void loginWithSlaveServer(const Request* request){
    std::shared_ptr<SNSService::Stub> slave_stub_ = get_slave_stub();
    if(!slave_stub_)
      return;

    Reply *r=new Reply();
    ClientContext *context = new ClientContext(); 
    slave_stub_->Login(context,*request,r);
  }

  bool documentPresent(const std::string& filename) {
      std::ifstream file(filename);
      return file.good();
  }

  void followWithSlaveServer(const Request* request){
    std::shared_ptr<SNSService::Stub> slave_stub_ = get_slave_stub();
    if(!slave_stub_) 
      return;

    Reply *r=new Reply();
    ClientContext *context = new ClientContext(); 
    slave_stub_->Follow(context,*request,r);
  }

  void unfollowWithSlaveServer(const Request* request){
    std::shared_ptr<SNSService::Stub> slave_stub_ = get_slave_stub();
    if(!slave_stub_) 
      return;

    Reply *r=new Reply();
    ClientContext *context = new ClientContext(); 
    slave_stub_->UnFollow(context,*request,r);
  }

  std::map<std::string, std::set<std::string>> msgset;

  void transmit20topmessages(ServerReaderWriter<Message, Message>* stream,Message m) {

    // Get the timeline document file for the specified username
    std::string document = getTimelineFileName(m.username());
    std::ifstream timeline_file(document);
        
    int position = 0;

    // Read lines from the timeline file until 20 messages are transmitted or the file ends
    std::string line;
    while (position<20 && std::getline(timeline_file, line)) {
      position = position + 1;
      std::cout<<"INFO: "<<line<<"\n";

      // Ensure a set exists for the user's messages
      if(msgset.find(m.username()) == msgset.end()){
        msgset[m.username()] = *(new std::set<std::string>());
      }

      // Retrieve the set of messages for the user
      std::set<std::string> mset=msgset[m.username()];
      if(mset.find(line) != mset.end()) continue;
      mset.insert(line);

      // Tokenize the line into words
      std::istringstream iss(line);
      std::vector<std::string> words;
      std::string word;
      while (iss >> word) {
        words.push_back(word);
      }

      Message msg;
      msg.set_username(words[0]);
      msg.set_msg(words[1]);

      if(m.username() == msg.username()) continue;

      // Get the usernames for comparison and the follow time
      std::string clientName = m.username();
      std::string followingClientName = msg.username();
      std::string followClientTime = obtainFollowTime(clientName,followingClientName);
      std::cout<<clientName<<" follow "<<followingClientName<<" in "<<followClientTime<<"\n";
      
      // Skip the message if it should not be included in the timeline
      if(followClientTime > words[2]) {
        std::cout<<"Timeline has been skipped, "<<line<<" "<<followClientTime <<" > "<< words[2]<<std::endl;
        continue;
      }

      // Create a Timestamp object from the timestamp string
      google::protobuf::Timestamp *timestamp = new google::protobuf::Timestamp();
      if (google::protobuf::util::TimeUtil::FromString(words[2], timestamp)) {
        std::cout << "Timestamp: " << timestamp->DebugString() << std::endl;
      } else {
        std::cerr << "FAIL!" << std::endl;
      }

      msg.set_allocated_timestamp(timestamp);

      stream->Write(msg);          
    } 
  }

  // Function to transmit a message to a follower through a stream
  void transmitToFollower(std::string record,ServerReaderWriter<Message, Message>* stream,Message m) {
    // Check if the stream is valid
    if(stream == nullptr){
      std::cout<<"Error sending to the follower"<<"\n";
      return;
    }

    // Write the message to the follower through the stream
    stream->Write(m);
  }

  // Function to obtain the follow time between a follower and following
  std::string obtainFollowTime(std::string follower,std::string following) {
    for(Client c:client_db) {
      // Skip clients that are not the specified follower
      if(c.username != follower) continue;
      for(int i=0;i<c.client_following.size();i++){
        // Check if the current following client matches the specified following
        if(c.client_following[i]->username == following){
          // Return the follow time for the specified follower and following
          return c.client_following_time[i];
        }
      }
    }

    return "";
  }

  void TimelineWithSlaveServer(Message* message){
    std::shared_ptr<SNSService::Stub> slave_stub_ = get_slave_stub();
    if(!slave_stub_) return;

    ClientContext *context = new ClientContext();  
    std::shared_ptr<ClientReaderWriter<Message,Message>> rw = slave_stub_->Timeline(context);
    Message m = MakeMessage(message->username(),message->msg());
    rw->Write(m);
  }

  // Function to persist a record to the timeline document for a user
  void persistTimelineDocument(std::string record,std::string name) {
    std::string timelineFile = getTimelineFileName(name);

    // Loop until successfully acquiring a lock on the timeline file
    for (;;) {
      int element = lockFile(timelineFile);
      if(element == 0) break;
    }
    
    // Read existing lines from the timeline file
    std::ifstream timelinefile(timelineFile);

    std::vector<std::string> lines;
    std::string line;

    // Populate the vector with existing lines
    while (std::getline(timelinefile, line)) {
      lines.push_back(line);
    }
    timelinefile.close();

    // Insert the new record at the beginning of the lines vector
    lines.insert(lines.begin(), record);

    // Write the modified lines vector back to the timeline file
    std::ofstream timeline_file_stream(timelineFile);
    for (const std::string& modified_line : lines) {
      timeline_file_stream << modified_line << std::endl;
    }
    timeline_file_stream.close();

    // Release the lock on the timeline file
    unlockFile(timelineFile);
  }

  Status Timeline(ServerContext* context, ServerReaderWriter<Message, Message>* stream) override {
    Message m;
    
    // Continuously read messages from the stream
    while(stream->Read(&m)) {

      // Fetch all clients and retrieve follower and following information
      fetchAllClients();
      retrieveFollowerAndFollowingFromDocument();

      // Check if the message is a join_timeline request
      if(m.msg() == "join_timeline") {

        std::cout<<"USER: "<<m.username()<<" JOIN TIMELINE!"<<std::endl;
        
        // Update the user's stream in the client database
        for (int i=0; i<client_db.size(); i++) {
           if(client_db[i].username == m.username()) {
            client_db[i].stream=stream;
           }
        }

        // Get the timeline document name for the user
        std::string documentName = getTimelineFileName(m.username());
        std::cout<<"Recording any changes in the file: "<<documentName<<std::endl;

        // Get the client's name and version number
        std::string clientName = m.username();
        int versionNumber = getVersionOfUser(clientName);

        // Start a separate thread to monitor the timeline file
        std::thread write([documentName,stream,clientName,versionNumber] {
          monitorFile(documentName,stream,clientName, versionNumber);
        });
        write.detach();

        // Transmit the top 20 messages to the user
        transmit20topmessages(stream,m);

        continue;
      }
      
      std::string time = google::protobuf::util::TimeUtil::ToString(m.timestamp());
      std::string entry = m.username()+" "+m.msg()+" "+time;

      // Iterate through clients to update timelines
      for(int i=0;i<client_db.size();i++){
        if(client_db[i].username != m.username()){
          continue;
        }

        for(int j=0;j<client_db[i].client_followers.size();j++) {
          // Persist the timeline entry for the user
          persistTimelineDocument(entry, m.username());

          int clientA=(stoi(m.username())-1)%3+1;
          int clientB=(stoi( client_db[i].client_followers[j]->username) - 1)%3+1;

          // Check if clients are in the same cluster
          if(clientA == clientB) {
            std::cout<<"Clients are residing in the similar cluster"<<std::endl;
            // Persist the timeline entry for the follower
            persistTimelineDocument(entry, client_db[i].client_followers[j]->username);
          }
        }
      }

      // Transmit the timeline update to slave servers
      TimelineWithSlaveServer(&m);
    }

    return Status::OK;
  }

  Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
  }

};

// Function to send heartbeat messages at regular intervals
void Heartbeat(std::shared_ptr<CoordService::Stub> stub_,std::string *serverID,std::string *clusterID){
  for (;;) {
    ClientContext *context = new ClientContext();  

    // Create a ServerInfo object with server and cluster information
    ServerInfo *info = new ServerInfo();
    info->set_serverid(atoi(serverID->c_str()));
    info->set_clusterid(atoi(clusterID->c_str()));

    // Create a Confirmation object for the reply
    Confirmation *reply=new Confirmation();

    // Send heartbeat message to the coordinator server
    stub_->Heartbeat(context,*info,reply);

    // Sleep for 1 second before sending the next heartbeat
    sleep(1);
  }
}

void RunServer(std::string port_no, std::string clusterID, std::string serverID, std::string coordinatorIP, std::string coordinatorPort) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service;

  service.clusterID=clusterID;
  service.serverID=serverID;
  service.coordinatorIP=coordinatorIP;
  service.coordinatorPort=coordinatorPort;

  // Create a ServerInfo object with cluster and server information
  ServerInfo *info = new ServerInfo();
  int id = atoi(clusterID.c_str());
  info->set_clusterid(id);

  id=atoi(serverID.c_str());
  info->set_serverid(id);

  info->set_hostname("127.0.0.1");
  info->set_port(port_no);

  info->set_servertype("server");

  std::string login_info(coordinatorIP + ":" + coordinatorPort);
  grpc::ChannelArguments channel_args;
  std::shared_ptr<Channel> channel = grpc::CreateCustomChannel(login_info, grpc::InsecureChannelCredentials(), channel_args);
  service.stub_ = CoordService::NewStub(channel);

  std::shared_ptr<CoordService::Stub> stub = service.stub_;

  // Create a new client context for communication
  ClientContext *context = new ClientContext();  
  Confirmation *reply=new Confirmation();

  // Check if the server exists and get its status
  stub->Exist(context,*info,reply);

  // If the server was restarted, perform resynchronization
  if(reply->status()){
    std::cout<<"Server was restarted. Its getting resynced!"<<std::endl;

    // Get the synchronizer information from the coordinator
    context = new ClientContext(); 
    ID id;
    id.set_id(stoi(clusterID));
    ServerInfo sync;
    stub->GetSynchronizer(context,id,&sync);

    // Create a connection information string for the synchronizer
    std::string cinfo(sync.hostname() + ":" + sync.port());
    grpc::ChannelArguments args;
    std::shared_ptr<Channel> syncchannel = grpc::CreateCustomChannel(cinfo, grpc::InsecureChannelCredentials(), args);
    std::shared_ptr<SynchService::Stub> stub_sync = SynchService::NewStub(syncchannel);

    // Create a new client context for synchronization
    context = new ClientContext(); 
    ResynchServerRequest req;
    req.set_serverid(stoi(serverID));

    ResynchServerResponse r;
    stub_sync->ResynchServer(context,req,&r);
  }

  context = new ClientContext(); 
  stub->Create(context,*info,reply);

  // Start a separate thread for the heartbeat functionality
  std::thread hb([stub,serverID,clusterID] {
      std::string cid=clusterID;
      std::string sid=serverID;
      Heartbeat(stub,&sid,&cid);
  });
	hb.detach();

  // Configure the server builder
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  // Build and start the server
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  // Wait for the server to finish
  server->Wait();
}

int main(int argc, char** argv) {

  std::string port = "3010";

  std::string clusterID="1";
  std::string serverID="1";
  std::string coordinatorIP="127.0.0.1";
  std::string coordinatorPort="3010";
  
  int opt = 0;
  while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1){
    switch(opt) {
      case 'c':
          clusterID=optarg;break;
      case 's':
          serverID=optarg;break;
      case 'h':
          coordinatorIP=optarg;break;   
      case 'k':
          coordinatorPort=optarg;break;  
      case 'p':
          port = optarg;break;    
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  std::cout<<"clusterID:"<<clusterID<<" serverID:"<<serverID<<std::endl;
  RunServer(port,clusterID,serverID,coordinatorIP,coordinatorPort);

  return 0;
}
