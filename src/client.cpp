/*
* Redox - A modern, asynchronous, and wicked fast C++11 client for Redis
*
*    https://github.com/hmartiro/redox
*
* Copyright 2015 - Hayk Martirosyan <hayk.mart at gmail dot com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#include <signal.h>
#include "client.hpp"

using namespace std;

void *hi_malloc(unsigned long size){
    return malloc(size);
}

namespace {

template<typename tev, typename tcb>
void redox_ev_async_init(tev ev, tcb cb)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
    ev_async_init(ev,cb);
#pragma GCC diagnostic pop
}

template<typename ttimer, typename tcb, typename tafter, typename trepeat>
void redox_ev_timer_init(ttimer timer, tcb cb, tafter after, trepeat repeat)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
    ev_timer_init(timer,cb,after,repeat);
#pragma GCC diagnostic pop
}

} // anonymous

namespace redox {

Redox::Redox(ostream &log_stream, log::Level log_level)
    : logger_(log_stream, log_level), evloop_(nullptr) {}

bool Redox::connect(const string &host, const int port,
                    function<void(int)> connection_callback) {

  host_ = host;
  port_ = port;
  user_connection_callback_ = connection_callback;

  if (!initEv())
    return false;

  // Connect over TCP
  ctx_ = redisAsyncConnect(host.c_str(), port);

  if (!initHiredis())
    return false;

  event_loop_thread_ = thread([this] { runEventLoop(); });

  // Block until connected and running the event loop, or until
  // a connection error happens and the event loop exits
  {
    unique_lock<mutex> ul(running_lock_);
    running_waiter_.wait(ul, [this] {
      lock_guard<mutex> lg(connect_lock_);
      return running_ || connect_state_ == CONNECT_ERROR;
    });
  }

  // Return if succeeded
  return getConnectState() == CONNECTED;
}

bool Redox::connectUnix(const string &path, function<void(int)> connection_callback) {

  path_ = path;
  user_connection_callback_ = connection_callback;

  if (!initEv())
    return false;

  // Connect over unix sockets
  ctx_ = redisAsyncConnectUnix(path.c_str());

  if (!initHiredis())
    return false;

  event_loop_thread_ = thread([this] { runEventLoop(); });

  // Block until connected and running the event loop, or until
  // a connection error happens and the event loop exits
  {
    unique_lock<mutex> ul(running_lock_);
    running_waiter_.wait(ul, [this] {
      lock_guard<mutex> lg(connect_lock_);
      return running_ || connect_state_ == CONNECT_ERROR;
    });
  }

  // Return if succeeded
  return getConnectState() == CONNECTED;
}

void Redox::disconnect() {
  stop();
  wait();
}

void Redox::stop() {
  to_exit_ = true;
  logger_.debug() << "stop() called, breaking event loop";
  ev_async_send(evloop_, &watcher_stop_);
}

void Redox::wait() {
  unique_lock<mutex> ul(exit_lock_);
  exit_waiter_.wait(ul, [this] { return exited_; });
}

Redox::~Redox() {

  // Bring down the event loop
  if (getRunning()) {
    stop();
  }

  if (event_loop_thread_.joinable())
    event_loop_thread_.join();

  if (evloop_ != nullptr)
    ev_loop_destroy(evloop_);
}

void Redox::connectedCallback(const redisAsyncContext *ctx, int status) {
  Redox *rdx = (Redox *)ctx->data;

  if (status != REDIS_OK) {
    rdx->logger_.fatal() << "Could not connect to Redis: " << ctx->errstr;
    rdx->logger_.fatal() << "Status: " << status;
    rdx->setConnectState(CONNECT_ERROR);

  } else {
    rdx->logger_.info() << "Connected to Redis.";
    // Disable hiredis automatically freeing reply objects
    ctx->c.reader->fn->freeObject = [](void *reply) {};
    rdx->setConnectState(CONNECTED);
  }

  if (rdx->user_connection_callback_) {
    rdx->user_connection_callback_(rdx->getConnectState());
  }
}

void Redox::disconnectedCallback(const redisAsyncContext *ctx, int status) {

  Redox *rdx = (Redox *)ctx->data;

  if (status != REDIS_OK) {
    rdx->logger_.error() << "Disconnected from Redis on error: " << ctx->errstr;
    rdx->setConnectState(DISCONNECT_ERROR);
  } else {
    rdx->logger_.info() << "Disconnected from Redis as planned.";
    rdx->setConnectState(DISCONNECTED);
  }

  rdx->stop();
  if (rdx->user_connection_callback_) {
    rdx->user_connection_callback_(rdx->getConnectState());
  }
}

bool Redox::initEv() {
  signal(SIGPIPE, SIG_IGN);
  evloop_ = ev_loop_new(EVFLAG_AUTO);
  if (evloop_ == nullptr) {
    logger_.fatal() << "Could not create a libev event loop.";
    setConnectState(INIT_ERROR);
    return false;
  }
  ev_set_userdata(evloop_, (void *)this); // Back-reference
  return true;
}

bool Redox::initHiredis() {

  ctx_->data = (void *)this; // Back-reference

  if (ctx_->err) {
    logger_.fatal() << "Could not create a hiredis context: " << ctx_->errstr;
    setConnectState(INIT_ERROR);
    return false;
  }

  // Attach event loop to hiredis
  if (redisLibevAttach(evloop_, ctx_) != REDIS_OK) {
    logger_.fatal() << "Could not attach libev event loop to hiredis.";
    setConnectState(INIT_ERROR);
    return false;
  }

  // Set the callbacks to be invoked on server connection/disconnection
  if (redisAsyncSetConnectCallback(ctx_, Redox::connectedCallback) != REDIS_OK) {
    logger_.fatal() << "Could not attach connect callback to hiredis.";
    setConnectState(INIT_ERROR);
    return false;
  }

  if (redisAsyncSetDisconnectCallback(ctx_, Redox::disconnectedCallback) != REDIS_OK) {
    logger_.fatal() << "Could not attach disconnect callback to hiredis.";
    setConnectState(INIT_ERROR);
    return false;
  }

  return true;
}

void Redox::noWait(bool state) {
  if (state)
    logger_.info() << "No-wait mode enabled.";
  else
    logger_.info() << "No-wait mode disabled.";
  nowait_ = state;
}

void breakEventLoop(struct ev_loop *loop, ev_async *async, int revents) {
  ev_break(loop, EVBREAK_ALL);
}

int Redox::getConnectState() {
  lock_guard<mutex> lk(connect_lock_);
  return connect_state_;
}

void Redox::setConnectState(int connect_state) {
  {
    lock_guard<mutex> lk(connect_lock_);
    connect_state_ = connect_state;
  }
  connect_waiter_.notify_all();
}

int Redox::getRunning() {
  lock_guard<mutex> lg(running_lock_);
  return running_;
}
void Redox::setRunning(bool running) {
  {
    lock_guard<mutex> lg(running_lock_);
    running_ = running;
  }
  running_waiter_.notify_one();
}

int Redox::getExited() {
  lock_guard<mutex> lg(exit_lock_);
  return exited_;
}
void Redox::setExited(bool exited) {
  {
    lock_guard<mutex> lg(exit_lock_);
    exited_ = exited;
  }
  exit_waiter_.notify_one();
}

void Redox::runEventLoop() {

  // Events to connect to Redox
  ev_run(evloop_, EVRUN_ONCE);
  ev_run(evloop_, EVRUN_NOWAIT);

  // Block until connected to Redis, or error
  {
    unique_lock<mutex> ul(connect_lock_);
    connect_waiter_.wait(ul, [this] { return connect_state_ != NOT_YET_CONNECTED; });

    // Handle connection error
    if (connect_state_ != CONNECTED) {
      logger_.warning() << "Did not connect, event loop exiting.";
      setExited(true);
      setRunning(false);
      return;
    }
  }

  // Set up asynchronous watcher which we signal every
  // time we add a command
  redox_ev_async_init(&watcher_command_, processQueuedCommands);
  ev_async_start(evloop_, &watcher_command_);

  // Set up an async watcher to break the loop
  redox_ev_async_init(&watcher_stop_, breakEventLoop);
  ev_async_start(evloop_, &watcher_stop_);

  // Set up an async watcher which we signal every time
  // we want a command freed
  redox_ev_async_init(&watcher_free_, freeQueuedCommands);
  ev_async_start(evloop_, &watcher_free_);

  setRunning(true);

  // Run the event loop, using NOWAIT if enabled for maximum
  // throughput by avoiding any sleeping
  while (!to_exit_) {
    if (nowait_) {
      ev_run(evloop_, EVRUN_NOWAIT);
    } else {
      ev_run(evloop_);
    }
  }

  logger_.info() << "Stop signal detected. Closing down event loop.";

  // Signal event loop to free all commands
  freeAllCommands();

  // Wait to receive server replies for clean hiredis disconnect
  this_thread::sleep_for(chrono::milliseconds(10));
  ev_run(evloop_, EVRUN_NOWAIT);

  if (getConnectState() == CONNECTED) {
    redisAsyncDisconnect(ctx_);
  }

  // Run once more to disconnect
  ev_run(evloop_, EVRUN_NOWAIT);

  long created = commands_created_;
  long deleted = commands_deleted_;
  if (created != deleted) {
    logger_.error() << "All commands were not freed! " << deleted << "/"
                    << created;
  }

  // Let go for block_until_stopped method
  setExited(true);
  setRunning(false);

  logger_.info() << "Event thread exited.";
}

void Redox::processQueuedCommands(struct ev_loop *loop, ev_async *async, int revents) {

  Redox *rdx = (Redox *)ev_userdata(loop);

  lock_guard<mutex> lg(rdx->queue_guard_);

  while (!rdx->command_queue_.empty()) {

    auto c = rdx->command_queue_.front();
    rdx->command_queue_.pop();
    c->processQueuedCommand_t();
  }
}

void Redox::freeQueuedCommands(struct ev_loop *loop, ev_async *async, int revents) {

  Redox *rdx = (Redox *)ev_userdata(loop);

  lock_guard<mutex> lg(rdx->free_queue_guard_);

  while (!rdx->commands_to_free_.empty()) {
    auto c = rdx->commands_to_free_.front();
    rdx->commands_to_free_.pop();
    c->freeReply_t();
    rdx->deregisterCommand();
    delete c;
  }
}

long Redox::freeAllCommands() {
  lock_guard<mutex> lg(free_queue_guard_);
  lock_guard<mutex> lg2(queue_guard_);

  long len = 0;

  while (!command_queue_.empty()) {
    auto c = command_queue_.front();
    command_queue_.pop();
    c->freeReply_t();
    len++;
    delete c;
  }

  commands_deleted_ += len;

  return len;
}

// ----------------------------
// Helpers
// ----------------------------

string Redox::vecToStr(const vector<string> &vec, const char delimiter) {
  string str;
  for (size_t i = 0; i < vec.size() - 1; i++)
    str += vec[i] + delimiter;
  str += vec[vec.size() - 1];
  return str;
}

vector<string> Redox::strToVec(const string &s, const char delimiter) {
  vector<string> vec;
  size_t last = 0;
  size_t next = 0;
  while ((next = s.find(delimiter, last)) != string::npos) {
    vec.push_back(s.substr(last, next - last));
    last = next + 1;
  }
  vec.push_back(s.substr(last));
  return vec;
}

void Redox::command(const vector<string> &cmd) { command<redisReply *>(cmd, nullptr); }
void Redox::command(formated_string cmd) { command<redisReply *>(cmd, nullptr); }

bool Redox::commandSync(const vector<string> &cmd) {
  auto &c = commandSync<redisReply *>(cmd);
  bool succeeded = c.ok();
  c.free();
  return succeeded;
}

string Redox::get(const string &key) {

  Command<char *> &c = commandSync<char *>({"GET", key});
  if (!c.ok()) {
    throw runtime_error("[FATAL] Error getting key " + key + ": Status code " +
                        to_string(c.status()));
  }
  string reply = c.reply();
  c.free();
  return reply;
}

bool Redox::set(const string &key, const string &value) { return commandSync({"SET", key, value}); }

bool Redox::del(const string &key) { return commandSync({"DEL", key}); }

void Redox::publish(const string &topic, const string &msg) {
  command<redisReply *>({"PUBLISH", topic, msg});
}

} // End namespace redis
