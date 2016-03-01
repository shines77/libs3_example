

#include <stdlib.h>
#include <stdio.h>

#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "libs3.h"

namespace rocksdb {
class DB {
public:
    DB()  {}
    ~DB() {}

    void destroy() {};
};
}

namespace utils {
class condition {
public:
    condition()  {}
    ~condition() {}
};
class backup_status {
public:
    backup_status()  {}
    ~backup_status() {}
};
}

struct s3_string_t {
    uint32_t     length;
    const char * data;
};

struct compact_object_summary_t {
    struct s3_string_t key;
    struct s3_string_t eTag;
    uint64_t lastModified;
    uint64_t size;
};

struct object_summary_t {
    std::string key;
    std::string eTag;
    uint64_t lastModified;
    uint64_t size;
};

enum cluster_type_t {
    CLUSTER_TYPE_UNKNOWN,
    CLUSTER_TYPE_ONLINE,
    CLUSTER_TYPE_BACKUP
};

struct finish_callback_t {
    virtual void on_finished(int status) = 0;
};

struct list_finish_callback_t : public finish_callback_t {
    virtual void on_finished(int status) override {
        // TODO:
    }
};

enum running_status {
    unknown,
    initializing,
    initialized,
    startting,
    startted,
    running,
    stopping,
    stopped,
    terminating,
    terminated
};

namespace System {

void sleep(uint32_t time_ms)
{
#if defined(_WIN32) || defined(WIN32)
    ::Sleep(time_ms);
#else
    ::Sleep(time_ms);
#endif
}

}

class list_object_summary_service
{
public:
    virtual int init() = 0;
    virtual int start() = 0;
    virtual int stop() = 0;
    virtual int uninit() = 0;
};

class list_object_summary_service_impl2 : public list_object_summary_service
{
    //
};

class list_object_summary_service_impl {
private:
    cluster_type_t cluster_type_;
    bool inited_;
    int status_;
    std::thread * worker_thread_;
    rocksdb::DB * db_;
    int last_error_;
    std::mutex mutex_;
    std::condition_variable suspend_cond_;
    std::condition_variable stop_cond_;
    std::condition_variable * p_stop_cond_;

public:
    list_object_summary_service_impl() : cluster_type_(CLUSTER_TYPE_UNKNOWN), inited_(false),
        status_(running_status::unknown), worker_thread_(nullptr), db_(nullptr),
        last_error_(0), mutex_(), suspend_cond_(), stop_cond_(), p_stop_cond_(nullptr)
    {
        //
    }

    virtual ~list_object_summary_service_impl()
    {
        this->uninit();
    }

    void nosql_db_destroy()
    {
        if (db_)
        {
            db_->destroy();
            delete db_;
            db_ = nullptr;
        }
    }

    void worker_thread_detach()
    {
        if (worker_thread_)
        {
            worker_thread_->detach();
            delete worker_thread_;
            worker_thread_ = nullptr;
        }
    }

    bool init(cluster_type_t cluster_type, const list_finish_callback_t & finish_cb)
    {
        mutex_.lock();
        status_ = running_status::initializing;
        inited_ = false;
        cluster_type_ = cluster_type;

        // Create the rocksdb::DB
        nosql_db_destroy();
        mutex_.unlock();

        rocksdb::DB * new_db = new rocksdb::DB();
        if (!new_db)
        {
            set_last_error(-1);
            return false;
        }
        mutex_.lock();
        db_ = new_db;

        // Create the worker thread
        worker_thread_detach();
        mutex_.unlock();

        std::function<void()> thread_func = std::bind(&list_object_summary_service_impl::worker_thread, this);
        std::thread * new_thread = new std::thread(thread_func);
        if (!new_thread)
        {
            nosql_db_destroy();
            set_last_error(-1);
            return false;
        }

        mutex_.lock();
        worker_thread_ = new_thread;

        status_ = running_status::initialized;
        inited_ = true;
        mutex_.unlock();
        return true;
    }

    bool uninit()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        worker_thread_detach();
        nosql_db_destroy();
        status_ = running_status::unknown;
        inited_ = false;
        return true;
    }

    bool is_inited() const { return inited_; }
    bool is_alive() const { return (worker_thread_ != nullptr); }
    bool is_running() const { return (status_ == running_status::running); }

    std::mutex & get_mutex() const { return *(const_cast<std::mutex *>(&mutex_)); }

    int start()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        status_ = running_status::startting;
        suspend_cond_.notify_one();
        status_ = running_status::startted;
        return 0;
    }

    int stop()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        status_ = running_status::stopping;
        printf("worker_thread stopping.\n");
        return 0;
    }

    int stop(std::condition_variable & stop_cond, int32_t timeout = -1)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        status_ = running_status::stopping;
        p_stop_cond_ = &stop_cond;
        printf("worker_thread stopping.\n");
        return 0;
    }

    int wait()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        stop_cond_.wait(lock);
        this->status_ = running_status::stopped;
        printf("worker_thread stopped.\n");
        return 0;
    }

    int wait_for(int32_t timeout = -1)
    {
        int status = -1;
        std::chrono::milliseconds timeout_ms = std::chrono::milliseconds(timeout);
        {
            std::unique_lock<std::mutex> lock(mutex_);
            std::cv_status wait_status = stop_cond_.wait_for(lock, timeout_ms);
            if (wait_status == std::cv_status::no_timeout)
                status = static_cast<int>(std::cv_status::no_timeout);
            else if (wait_status == std::cv_status::timeout)
                status = static_cast<int>(std::cv_status::timeout);
            this->status_ = running_status::stopped;
            printf("worker_thread stopped.\n");
        }
        return status;
    }

    int  suspend() { return 0; }
    int  resume() { return 0; }
    int  terminate(int32_t timeout) { return 0; }

    bool set_backup_status(utils::backup_status & status) { return true; }
    utils::backup_status get_last_backup_status() const { return utils::backup_status(); }

    int get_last_error() const { return last_error_; }
    void set_last_error(int error) { last_error_ = error; }

    int get_running_status() const { return status_; }

    bool is_online_cluster() const { return (cluster_type_ == CLUSTER_TYPE_ONLINE); }
    bool is_backup_cluster() const { return (cluster_type_ == CLUSTER_TYPE_BACKUP); }

    cluster_type_t get_cluster_type() const { return cluster_type_; }
    void set_cluster_type(cluster_type_t cluster_type) { cluster_type_ = cluster_type; }

protected:
    void set_running_status(int status) { status_ = status; }

    void worker_thread()
    {
        printf("worker_thread enter.\n");
        std::unique_lock<std::mutex> lock(mutex_);
        suspend_cond_.wait(lock);
        printf("worker_thread start.\n");
        do
        {
            if (this->status_ == running_status::startted
                || this->status_ == running_status::running)
            {
                this->status_ = running_status::running;
                printf("worker_thread running.\n");
                lock.unlock();

                // TODO: do some works.

                lock.lock();
                this->status_ = running_status::stopped;
            }
            else if (this->status_ == running_status::stopping ||
                this->status_ == running_status::terminating)
            {
#if 1
                stop_cond_.notify_one();
                if (this->status_ == running_status::stopping)
                {
                    printf("worker_thread will be stopped.\n");
                }
#else                
                if (p_stop_cond_)
                {
                    p_stop_cond_->notify_one();
                    if (this->status_ == running_status::stopping)
                    {
                        //this->status_ = running_status::stopped;
                        printf("worker_thread will be stopped22.\n");
                    }
                }
#endif
                lock.unlock();
                break;
            }
            else
            {
                lock.unlock();
                System::sleep(1);
                lock.lock();
            }
        } while (true);
        this->status_ = running_status::unknown;
        printf("worker_thread over.\n");
    }
};

int main(int argc, char * argv[])
{
    printf("S3 client!\n");
    list_finish_callback_t finish_callback;

    list_object_summary_service_impl * list_object_summary = new list_object_summary_service_impl();
    if (list_object_summary)
    {
        list_object_summary->init(CLUSTER_TYPE_ONLINE, finish_callback);
        utils::backup_status current_status;
        list_object_summary->set_backup_status(current_status);

        System::sleep(5000);

        list_object_summary->start();

        System::sleep(5000);

        list_object_summary->stop();
        list_object_summary->wait_for(10000);

        /*
        std::condition_variable stop_cond;
        list_object_summary->stop(stop_cond, 0);
        using namespace std::chrono;
        std::chrono::milliseconds timeout_ms = std::chrono::milliseconds(3000);

        {
            std::unique_lock<std::mutex> lock(list_object_summary->get_mutex());
            //stop_cond.wait(lock);
            stop_cond.wait_for(lock, timeout_ms);
            printf("list_object_summary have stopped.\n");
        }
        //*/
        printf("list_object_summary have stopped.\n");

        list_object_summary->uninit();

        delete list_object_summary;
    }

    system("pause");
    return 0;
}
