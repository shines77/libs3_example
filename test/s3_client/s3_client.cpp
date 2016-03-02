

#include <stdlib.h>
#include <stdio.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <atomic>
#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>

#include "libs3.h"

struct s3_string_t
{
    uint32_t length;
    const char * data;
};

struct compact_object_summary_t
{
    struct s3_string_t bucket;
    struct s3_string_t key;
    struct s3_string_t eTag;
    uint64_t lastModified;
    uint64_t size;
};

struct object_summary_t
{
    std::string bucket;
    std::string key;
    std::string eTag;
    uint64_t lastModified;
    uint64_t size;
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

namespace rocksdb {
class DB
{
public:
    DB()  {}
    ~DB() {}

    void destroy() {};
};
}

namespace timax {

enum cluster_type_t {
    CLUSTER_TYPE_UNKNOWN,
    CLUSTER_TYPE_ONLINE,
    CLUSTER_TYPE_BACKUP
};

class backup_status
{
public:
    backup_status()  {}
    ~backup_status() {}
};

struct finish_callback_t
{
    virtual void on_finished(int status) = 0;
};

struct list_finish_callback_t : public finish_callback_t
{
    virtual void on_finished(int status) override
    {
        // TODO:
    }
};

namespace s3 {

enum error_code
{
	E_S3_WORK_FIRST = -200,
	E_TIMEOUT,
	E_NO_TIMEOUT,
	E_MODULE_HAVE_NOT_INITIALIZED = -1,
	E_SUCCESS = 0,
	E_S3_WORK_LAST
};

enum work_result
{
    SUCCESS,
    FAILURE,
    DONE
};

} // namespace s3

enum running_status
{
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

static const char * get_running_status_string(int status)
{
    switch (status)
    {
        case running_status::unknown:
            return "unknown";
        case running_status::initializing:
            return "initializing";
        case running_status::initialized:
            return "initialized";
        case running_status::startting:
            return "startting";
        case running_status::startted:
            return "startted";
        case running_status::running:
            return "running";
        case running_status::stopping:
            return "stopping";
        case running_status::stopped:
            return "stopped";
        case running_status::terminating:
            return "terminating";
        case running_status::terminated:
            return "terminated";
        default:
            return "other status";
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

    bool is_inited() const { return inited_; }
    bool is_alive() const { return (worker_thread_ != nullptr); }
    bool is_running() const { return (status_ == running_status::running); }

    std::mutex & get_mutex() const { return *(const_cast<std::mutex *>(&mutex_)); }

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
            mutex_.lock();
            set_last_error(-1);
            mutex_.unlock();
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
            mutex_.lock();
            nosql_db_destroy();
            set_last_error(-1);
            mutex_.unlock();
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
        static const int32_t wait_for_ms = 30000;
        std::unique_lock<std::mutex> lock(mutex_);
        _stop();
        _wait_for(lock, wait_for_ms);
        _destroy();
        return true;
    }

    int start()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return _start();
    }

    int stop()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return _stop();
    }

    int stop(std::condition_variable & stop_cond, int32_t timeout = -1)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return _stop(stop_cond, timeout);
    }

    int wait()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return _wait(lock);
    }

    int wait_for(int32_t timeout = -1)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return _wait_for(lock, timeout);
    }

    int terminate(int32_t timeout = 0)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return _terminate(timeout);
    }

    int suspend() { return 0; }
    int resume() { return 0; }

    bool set_backup_status(backup_status & status) { return true; }
    backup_status get_last_backup_status() const { return backup_status(); }

    int get_last_error() const { return last_error_; }
    void set_last_error(int error) { last_error_ = error; }

    int get_running_status() const { return status_; }

    bool is_online_cluster() const { return (cluster_type_ == CLUSTER_TYPE_ONLINE); }
    bool is_backup_cluster() const { return (cluster_type_ == CLUSTER_TYPE_BACKUP); }

    cluster_type_t get_cluster_type() const { return cluster_type_; }
    void set_cluster_type(cluster_type_t cluster_type) { cluster_type_ = cluster_type; }

protected:
    void set_running_status(int status) { status_ = status; }

    void _destroy()
    {
        worker_thread_detach();
        nosql_db_destroy();
        status_ = running_status::unknown;
        inited_ = false;
    }

    void destroy()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        _destroy();
    }

    int _start()
    {
        if (is_inited() && is_alive())
        {
            if (status_ != running_status::startted && status_ != running_status::running)
            {
                status_ = running_status::startting;
                suspend_cond_.notify_one();
                status_ = running_status::startted;
            }
            return 0;
        }
        return -1;
    }

    int _stop()
    {
        if (!is_inited())
            return -1;

        if (status_ == running_status::startted || status_ == running_status::running)
        {
            status_ = running_status::stopping;
            printf("worker_thread stopping.\n");
        }
        else
        {
            printf("worker_thread _stop(): running_status = %s.\n", get_running_status_string(status_));
        }
        return 0;
    }

    int _stop(std::condition_variable & stop_cond, int32_t timeout = -1)
    {
        if (!is_inited())
            return -1;

        if (status_ == running_status::startted || status_ == running_status::running)
        {
            status_ = running_status::stopping;
            p_stop_cond_ = &stop_cond;
            printf("worker_thread stopping.\n");
        }
        else
        {
            printf("worker_thread _stop2(): running_status = %s.\n", get_running_status_string(status_));
        }
        return 0;
    }

    int _wait(std::unique_lock<std::mutex> & lock)
    {
        if (!is_inited())
            return -1;

        if (status_ == running_status::stopping || status_ == running_status::terminating)
        {
            stop_cond_.wait(lock);
            status_ = running_status::stopped;
            printf("worker_thread stopped.\n");
        }
        else
        {
            printf("worker_thread _wait(): running_status = %s.\n", get_running_status_string(status_));
        }
        return 0;
    }

    int _wait_for(std::unique_lock<std::mutex> & lock, int32_t timeout)
    {
        if (!is_inited())
            return -1;

        int wait_status = -2;
        if (status_ == running_status::stopping || status_ == running_status::terminating)
        {
            std::chrono::milliseconds timeout_ms = std::chrono::milliseconds(timeout);
            std::cv_status cond_status = stop_cond_.wait_for(lock, timeout_ms);
            if (cond_status == std::cv_status::no_timeout)
                wait_status = static_cast<int>(std::cv_status::no_timeout);
            else if (cond_status == std::cv_status::timeout)
                wait_status = static_cast<int>(std::cv_status::timeout);
            status_ = running_status::stopped;
            printf("worker_thread stopped.\n");
        }
        else
        {
            printf("worker_thread _wait_for(): running_status = %s.\n", get_running_status_string(status_));
        }
        return wait_status;
    }

    int _terminate(int32_t timeout = 0)
    {
        status_ = running_status::terminating;

        worker_thread_detach();
        nosql_db_destroy();

        status_ = running_status::terminated;
    }

    s3::work_result do_s3_list_object_summary()
    {
        printf("do_s3_list_object_summary() enter.\n");
        System::sleep(200);
        return s3::work_result::SUCCESS;
    }

    s3::work_result do_s3_work()
    {
        auto result = do_s3_list_object_summary();
        if (result == s3::work_result::SUCCESS)
        {
            // TODO: Record the work status
        }
        else if (result == s3::work_result::DONE)
        {
            // TODO: Call finish callback
        }
        else if (result == s3::work_result::FAILURE)
        {
            // Write the failure info to log
        }
        return result;
    }

    void worker_thread()
    {
        printf("worker_thread enter.\n");
        std::unique_lock<std::mutex> lock(mutex_);
        suspend_cond_.wait(lock);
        printf("worker_thread start.\n");
        do
        {
            if (status_ == running_status::startted || status_ == running_status::running)
            {
                status_ = running_status::running;
                printf("worker_thread running.\n");
                lock.unlock();

                // Do a s3 list object summary work.
                s3::work_result result = do_s3_work();

                lock.lock();
            }
            else if (status_ == running_status::stopping || status_ == running_status::terminating)
            {
#if 1
                stop_cond_.notify_one();
                if (status_ == running_status::stopping)
                {
                    printf("worker_thread will be stopped.\n");
                }
#else                
                if (p_stop_cond_)
                {
                    p_stop_cond_->notify_one();
                    if (status_ == running_status::stopping)
                    {
                        //status_ = running_status::stopped;
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

        lock.lock();
        status_ = running_status::unknown;
        lock.unlock();
        printf("worker_thread over.\n");
    }

private:
    //
};

} // namespace timax

using namespace timax;

int main(int argc, char * argv[])
{
    printf("S3 client!\n");
    list_finish_callback_t finish_callback;

    list_object_summary_service_impl * list_object_summary = new list_object_summary_service_impl();
    if (list_object_summary)
    {
        list_object_summary->init(CLUSTER_TYPE_ONLINE, finish_callback);
        timax::backup_status current_status;
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
