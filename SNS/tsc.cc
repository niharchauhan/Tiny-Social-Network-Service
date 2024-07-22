#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <grpc++/grpc++.h>
#include "client.h"
#include "google/protobuf/util/time_util.h"
#include <chrono>

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;


using csce438::CoordService;
using grpc::Channel;
using grpc::ClientContext;
using csce438::Confirmation;
using csce438::ServerInfo;
using csce438::ID;


void sig_ignore(int sig) {
  std::cout << "Signal caught " + sig<<"\n";
  exit(0);
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


class Client : public IClient
{
public:
  Client(const std::string& hname,
	 const std::string& uname,
	 const std::string& p)
    :hostname(hname), username(uname), port(p) {}

  
protected:
  virtual int connectTo();
  virtual int connectToServer();
  virtual IReply processCommand(std::string& input);
  virtual void processTimeline();
  virtual int updateServerType();

private:
  std::string hostname;
  std::string username;
  std::string port;

  std::string server_hostname;
  std::string server_port;
  std::string server_type;
  
  // You can have an instance of the client stub
  // as a member variable.
  std::unique_ptr<SNSService::Stub> stub_;
  std::shared_ptr<Channel> channel;

  std::shared_ptr<CoordService::Stub> coord_stub_;
  
  IReply Login();
  IReply List();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void   Timeline(const std::string &username);
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo()
{
  // ------------------------------------------------------------
  // In this function, you are supposed to create a stub so that
  // you call service methods in the processCommand/porcessTimeline
  // functions. That is, the stub should be accessible when you want
  // to call any service methods in those functions.
  // Please refer to gRpc tutorial how to create a stub.
  // ------------------------------------------------------------

    std::string login_info(hostname + ":" + port);
    grpc::ChannelArguments channel_args;
    std::shared_ptr<Channel> channel = grpc::CreateCustomChannel(login_info, grpc::InsecureChannelCredentials(), channel_args);
    std::shared_ptr<CoordService::Stub> stub = CoordService::NewStub(channel);
    coord_stub_ = stub;

    ClientContext *context = new ClientContext();  
    ID *id=new ID();
    id->set_id(atoi(username.c_str()));
    ServerInfo *reply = new ServerInfo();

    stub->GetServer(context,*id,reply);

    server_hostname = reply->hostname();
    server_port = reply->port();
    server_type = reply->type();

    if (server_type=="alive") {
      return connectToServer();
    }
    return -1;
    
}

int Client::connectToServer(){
    std::string login_info(server_hostname + ":" + server_port);

    // Create a custom gRPC channel with insecure credentials
    grpc::ChannelArguments channel_args;
    channel = grpc::CreateCustomChannel(login_info, grpc::InsecureChannelCredentials(), channel_args);

    // Create a stub for the SNS service using the established channel
    stub_ = SNSService::NewStub(channel);

    IReply r = Login();

    // Check if the gRPC status is OK
    if (!r.grpc_status.ok()) {
      return -1; // Return -1 if the gRPC status is not OK
    }

    // Check if the communication status is SUCCESS
    if (!r.comm_status==IStatus::SUCCESS) {
      return -1; // Return -1 if the communication status is not SUCCESS
    }
    return 1;
}

int Client::updateServerType(){
    ClientContext *context = new ClientContext();  
    ID *id=new ID();
    id->set_id(atoi(username.c_str()));
    ServerInfo *reply = new ServerInfo();

    coord_stub_->GetServer(context,*id,reply);

    server_hostname = reply->hostname();
    server_port = reply->port();
    server_type = reply->type();

    if (server_type!="alive") {
      return -1;
    }
}

IReply Client::processCommand(std::string& input) {
    IReply ire;
    
    std::istringstream iss(input);	
    std::vector<std::string> tokens;

    for (std::string token; iss >> token;) {
      tokens.push_back(token);
    }

    if (server_type!="alive") {
      updateServerType();
      if (server_type=="alive") {
        Login();
      }
    }

    updateServerType();
    if (server_type!="alive") {
      ire.comm_status = IStatus::FAILURE;
      return ire;
    }

    grpc_connectivity_state state = channel->GetState(true);
    if (state == 0 || state == GRPC_CHANNEL_TRANSIENT_FAILURE || state==GRPC_CHANNEL_SHUTDOWN) {
      server_type="down";
      ire.comm_status = IStatus::FAILURE;
      return ire;
    }

    if(tokens[0]=="TIMELINE"){
      ire.grpc_status = Status::OK;
      ire.comm_status = IStatus::SUCCESS;
    } else if(tokens[0]=="LIST"){
      ire = List();
    } else if(tokens[0]=="UNFOLLOW"){
      ire = UnFollow(tokens[1]);
    } else if(tokens[0]=="FOLLOW"){
      ire = Follow(tokens[1]);
    } else {
      // Default case or handle other values
    }

    return ire;
}


void Client::processTimeline()
{
    Timeline(username);
}

// List Command
IReply Client::List() {
    IReply ire;
   
    ClientContext *context = new ClientContext();  

    // Prepare the request with the client's username
    Request *request = new Request();
    request->set_username(username);

    // Create a ListReply object to store the server's response
    ListReply *reply = new ListReply();

    Status status = stub_->List(context,*request, reply);

    // Check if the RPC call was successful
    if(!status.ok()){
      ire.comm_status = IStatus::FAILURE;
      return ire;
    }
    
    // Populate the all_users vector in the IReply object with data from the server response
    int j = 0;
    while (j < reply->all_users_size()) {
      ire.all_users.insert(ire.all_users.begin(),reply->all_users()[j]);
      j++;
    }

    // Populate the followers vector in the IReply object with data from the server response
    int k = 0;
    while (k < reply->followers_size()) {
      ire.followers.insert(ire.followers.begin(),reply->followers()[k]);
      k++;
    }

    ire.comm_status = IStatus::SUCCESS;

    return ire;
}

// Follow Command        
IReply Client::Follow(const std::string& username2) {
    IReply ire; 
      
    ClientContext *context = new ClientContext();  

    Request *request = new Request();
    request->set_username(username);
    request->add_arguments(username2);

    Reply *reply = new Reply();

    Status status = stub_->Follow(context,*request, reply);

    ire.grpc_status = Status::OK;
    if(reply->msg()=="OK"){
      ire.comm_status = IStatus::SUCCESS;
    }
    else if(reply->msg() == "NO_TARGET"){
      ire.comm_status = IStatus::FAILURE_INVALID_USERNAME;
    }
    else if(reply->msg() == "Can't follow self"){
      ire.comm_status = IStatus::FAILURE_ALREADY_EXISTS;
    }
    else if(reply->msg() == "RE-FOLLOW"){
       ire.comm_status = IStatus::FAILURE_ALREADY_EXISTS;
    }
    else{
      ire.comm_status = IStatus::FAILURE_UNKNOWN;
    }

    return ire;
}

// UNFollow Command  
IReply Client::UnFollow(const std::string& username2) {
    IReply ire; 
      
    ClientContext *context = new ClientContext();  

    Request *request = new Request();
    request->set_username(username);
    request->add_arguments(username2);

    Reply *reply = new Reply();

    Status status = stub_->UnFollow(context,*request, reply);

    ire.grpc_status = Status::OK;
    if(reply->msg()=="OK"){
      ire.comm_status = IStatus::SUCCESS;
    }
    else if(reply->msg() == "NO_TARGET"){
      ire.comm_status = IStatus::FAILURE_INVALID_USERNAME;
    }
    else{
      ire.comm_status = IStatus::FAILURE_UNKNOWN;
    }

    return ire;
}

// Login Command  
IReply Client::Login() {
    IReply ire;
  
    ClientContext *context = new ClientContext();  

    Request *request = new Request();
    request->set_username(username);

    Reply *reply = new Reply();

    Status status = stub_->Login(context,*request, reply);

    ire.grpc_status = status;
    if(reply->msg()=="OK"){
      ire.comm_status = IStatus::SUCCESS;
    }
    else{
      ire.comm_status = IStatus::FAILURE_ALREADY_EXISTS;
    }
   
    return ire;
}

// Function to read messages from a shared reader-writer stream and display them
void TimelineRead(std::shared_ptr<grpc::ClientReaderWriter<Message,Message>> rw){
  Message *message = new Message();

  // Read messages from the reader-writer stream
  while(rw->Read(message)){
    std::time_t timeValue = static_cast<time_t>(message->timestamp().seconds());

    // Display the received message
    displayPostMessage(message->username(),message->msg(),timeValue);
  }
}

void TimelineWrite(std::shared_ptr<grpc::ClientReaderWriter<Message,Message>> rw,std::string *username){
  
  for (;;) {
    std::string input = getPostMessage();

    // Remove the trailing newline character
    input = input.substr(0, input.length() - 1);

    // Create a Message object using the MakeMessage function
    Message message = MakeMessage(*username, input);

    // Write the message using the rw (assuming rw is some kind of writer)
    rw->Write(message);
  }
}

// Timeline Command
void Client::Timeline(const std::string& username) {
    ClientContext *cntxt = new ClientContext();  

    // Create a bidirectional stream for communication
    std::shared_ptr<ClientReaderWriter<Message,Message>> rw = stub_->Timeline(cntxt);

    // Create a message to join the timeline
    Message message = MakeMessage(username,"join_timeline");
    rw->Write(message);

    // Pass the username and stream to a new thread for writing to the timeline
    std::string *s=new std::string(username);

    std::thread write([rw,s]{
      TimelineWrite(rw,s);
    });
	  write.detach();

    // Pass the stream to another thread for reading from the timeline
    std::thread read([rw]{
      TimelineRead(rw);
    });
	  read.detach();

    for (;;) {

    }
    
}


//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string hostname = "127.0.0.1";
  std::string hostport = "3010";
  std::string username = "default";

  std::string port = "3010";
    
  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:k:")) != -1){
    switch(opt) {
    case 'h':
      hostname = optarg;break;
    case 'u':
      username = optarg;break;
    case 'k':
      hostport=optarg;break;

    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }
  
  Client myc(hostname, username, hostport);
  
  signal(SIGINT, sig_ignore);

  myc.run();
  
  return 0;
}
