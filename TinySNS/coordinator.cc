#include <ctime>
#include <chrono>
#include <sys/stat.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include <vector>
#include <thread>
#include <mutex>
#include <map>
#include <algorithm>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include "sns.grpc.pb.h"
#include "snsCoordinator.grpc.pb.h"
#include "snsFollowSync.grpc.pb.h"
#include "time.h"

#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity);

using google::protobuf::Timestamp;
using google::protobuf::Duration;
// using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

using csce438::SNSService;
using csce438::Message;
using csce438::Request;
using csce438::Reply;

using snsCoordinator::SNSCoordinator;
using snsCoordinator::ServerType;
using snsCoordinator::User;
using snsCoordinator::ClusterId;
using snsCoordinator::Server;
using snsCoordinator::Users;
using snsCoordinator::FollowSyncs;
using snsCoordinator::Heartbeat;

using snsFollowSync::SNSFollowSync;
using snsFollowSync::Relation;
// using snsFollowSync::Users;
using snsFollowSync::Post;


using namespace std;

int hb = 1;

struct table{
    Server server;
    int status;
};

//these are the arrays of server clusters, change size to change cluster limit
table mservers[5] = {};
table sservers[5] = {};
table synchronizers[5] = {};
vector<int> all_users;
mutex mu_;

class SNSCoordinatorImpl final : public SNSCoordinator::Service {
    Status HandleHeartBeats(ServerContext* context, ServerReaderWriter<Heartbeat, Heartbeat>* stream) {
        log(INFO,"Serving HandleHeartBeats request");
        
        Heartbeat ping;
        ServerType st;
        table t;
        time_t time;

        stream->Read(&ping);
        auto const now = chrono::system_clock::now();
        time = chrono::system_clock::to_time_t(now);
        Timestamp ts = google::protobuf::util::TimeUtil::TimeTToTimestamp(time);
        ping.set_allocated_timestamp(&ts);
        stream->Write(ping);
        ping.release_timestamp();

        
        t.server.set_server_id(ping.server_id());
        t.server.set_server_ip(ping.server_ip());
        t.server.set_port_num(ping.server_port());
        t.server.set_server_type(ping.server_type());
        t.status = 1;
        
        bool found = false;
        int i = t.server.server_id();
        if (t.server.server_type() == 0) {
            if (mservers[i].server.server_ip() == t.server.server_ip()) {
                mservers[i].status = 1;   // master reactivated
                found = true;
            }
            if (!found) { mservers[i] = t;}
        }
        else if (t.server.server_type() == 1) {
            if (sservers[i].server.server_ip() == t.server.server_ip()) { found = true;}
            if (!found) { sservers[i] = t;}
        }
        else if (t.server.server_type() == 2) {
            if (synchronizers[i].server.server_ip() == t.server.server_ip()) { found = true;}
            if (!found) { synchronizers[i] = t;}
        }
        // cout << "connected with server " << t.server.server_type() << ":" << t.server.server_id()  << endl;
        
        // Heartbeat ping;
        log(INFO,"Initiating Heartbeats with server");
        while(stream->Read(&ping)) {
            sleep(hb);
            if (!stream->Write(ping)) {
                int idx = ping.server_id();
                if (t.server.server_type() == 0) { 
                    mservers[idx].status = 0;
                }
                else if (t.server.server_type() == 1) { 
                    sservers[idx].status = 0;
                }
                break;
            }
        }
        log(INFO,"Server disconnected");
        return Status::OK;
    }

    Status GetFollowSyncsForUsers(ServerContext* context, const Users* users, FollowSyncs* followSyncs) {
        log(INFO,"Serving GetFollowSyncsForUsers request");
        for (int i = 0; i < users->users_size(); ++i) {
            int sid = -1;
            if ((users->users()[i] % 3) + 1 != -1) {
                sid = (users->users()[i] % 3) + 1;
            }
            followSyncs->add_users(users->users()[i]);
            followSyncs->add_follow_syncs(sid - 1);
            followSyncs->add_follow_sync_ip(synchronizers[sid - 1].server.server_ip());
            followSyncs->add_port_nums(synchronizers[sid - 1].server.port_num());
        }
        return Status::OK;
    }

    Status GetServer(ServerContext* context, const User* user, Server* server) {
        log(INFO,"Serving GetServer request");
        // store status in binary for logic
        int sid = -1;
        bool found;
        if ((user->user_id() % 3) + 1 != -1) {
            sid = (user->user_id() % 3) + 1;
        }
        if (mservers[sid - 1].status) {
            server->CopyFrom(mservers[sid - 1].server);
        }
        else {
            server->CopyFrom(sservers[sid - 1].server);
        }
        for (int i = 0; i < all_users.size(); ++i) {
            if (user->user_id() == all_users[i]) {
                found = true;
                break;
            }
        }
        if (!found) { //add user to global list of users
            all_users.push_back(user->user_id());
            sort(all_users.begin(), all_users.end());
        }
        return Status::OK;
    }

    Status GetSlave(ServerContext* context, const ClusterId* clusterId, Server* server) {
        log(INFO,"Serving GetSlave request");
        int sid = -1;
        if ((clusterId->cluster() % 3) + 1 != -1) {
            sid = (clusterId->cluster() % 3) + 1;
        }
        server->CopyFrom(sservers[sid - 1].server);
        return Status::OK;
    }

    Status GetAllUsers(ServerContext* context, const ClusterId* clusterId, Users* users) {
        log(INFO,"Serving GetAllUsers request");
        for (int i = 0; i < all_users.size(); ++i) {
            users->add_users(all_users[i]);
        }
        return Status::OK;
    }
};


void RunCoordinator(std::string port_no) {
    log(INFO,"Initializing Coordinator");
    SNSCoordinatorImpl service;
    ServerBuilder builder;

    builder.AddListeningPort(port_no, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    unique_ptr<grpc::Server> coord(builder.BuildAndStart());

    cout << "Coordinator listening on port: " << port_no << endl;
    coord->Wait();

}

int main(int argc, char** argv) {
    
    string def = "0.0.0.0:";
    string port = "9000";

    int opt = 0;
    while ((opt = getopt(argc, argv, "p:h:")) != -1){
        switch(opt) {
            case 'p':
                port = optarg;
                break;
            case 'h':
                hb = stoi(optarg);
                break;
            default:
                cerr << "Invalid Command Line Argument\n";
        }
    }

    std::string log_file_name = std::string("Coordinator");
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Coordinator starting...");
    RunCoordinator(def + port);
    return 0;
}