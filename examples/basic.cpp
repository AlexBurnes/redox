/**
* Basic use of Redox to set and get a Redis key.
*/

#include <iostream>
#include <redox.hpp>

using namespace std;
using redox::Redox;
using redox::Command;
using redox::Subscriber;

int main(int argc, char* argv[]) {

  Redox rdx;
  cout << "connecting" << std::endl;
  if(!rdx.connect("localhost", 6380)) return 1;

  cout << "connected, del occupation" << std::endl;

  if (!rdx.del("occupation")) {
    cerr << "Failed del occupation" << std::endl;
    //return 1;
  }

  cout << "set occupation" << std::endl;

  if(!rdx.set("occupation", "carpenter")) {
    cerr << "Failed to set key!" << endl;
    //return 1;
  }

  cout << "key = \"occupation\", value = \"" << rdx.get("occupation") << "\"" << endl;

  return 0;
}
