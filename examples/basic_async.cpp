/**
* Basic use of Redox to set and get a Redis key.
*/

#include <iostream>
#include <redox.hpp>

using namespace std;
using redox::Redox;
using redox::Command;
using redox::Subscriber;

bool connected;
bool done;

int connect_callback(int connect_state) {
    cout << "redis connect callback connect state " << to_string(connect_state) << endl;
    if (connect_state == redox::Redox::CONNECTED) {
        connected = true;
        return 1;
    }
    if (connect_state == redox::Redox::DISCONNECTED) {
        connected = false;
        return 1;
    }

    if (connect_state == redox::Redox::CONNECT_ERROR) {
        cerr << "failed connect" << endl;
        connected = false;
        return 0;
    }

    if (connect_state == redox::Redox::DISCONNECT_ERROR) {
        cerr << "failed connect" << endl;
        connected = false;
        return 0;
    }

    return 1;
}

int main(int argc, char* argv[]) {

    connected = false;
    done = false;

    long int tries = 0;

    while(!done) {
        ++tries;
        Redox rdx;
        cout << "connecting try " << to_string(tries) << std::endl;
        if(!rdx.connect("localhost", 6380, connect_callback)) {
            cerr << "main: failed connect" << endl;
            continue;
        }

        if (!connected) {
            cerr << "main: not connected" << endl;
            continue;
        }

        cout << "connected, del occupation" << std::endl;

        if (!rdx.del("occupation")) {
            cerr << "Failed del occupation" << std::endl;
            continue;
        }

        cout << "set occupation" << std::endl;

        if(!rdx.set("occupation", "carpenter")) {
            cerr << "Failed to set key!" << endl;
            continue;
        }

        cout << "key = \"occupation\", value = \"" << rdx.get("occupation") << "\"" << endl;
        done = true;

    }

    return 0;
}
