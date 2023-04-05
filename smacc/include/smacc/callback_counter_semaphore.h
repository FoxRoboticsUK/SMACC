/*****************************************************************************************************************
 * ReelRobotix Inc. - Software License Agreement      Copyright (c) 2018
 * 	 Authors: Pablo Inigo Blasco, Brett Aldrich
 *
 ******************************************************************************************************************/

#pragma once
#include <boost/signals2.hpp>
#include <condition_variable>
#include <mutex>
#include <ros/ros.h>

namespace smacc
{
class CallbackCounterSemaphore {
public:
    CallbackCounterSemaphore(std::string name, int count = 0);
    bool acquire() 
    {
        std::unique_lock<std::mutex> lock(mutex_);
        ROS_DEBUG("[CallbackCounterSemaphore] acquire callback %s %ld",name_.c_str(), (long)this);

        if(finalized)
        {
            ROS_DEBUG("[CallbackCounterSemaphore] callback rejected %s %ld",name_.c_str(), (long)this);
            return false;
        }

        ++count_;
        cv_.notify_one();

        ROS_DEBUG("[CallbackCounterSemaphore] callback accepted %s %ld",name_.c_str(), (long)this);
        return true;
    }

    void release()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        --count_;
        cv_.notify_one();

        ROS_DEBUG("[CallbackCounterSemaphore] callback finished %s %ld",name_.c_str(), (long)this);
    }

    void finalize() 
    {
        std::unique_lock<std::mutex> lock(mutex_);

        while (count_ > 0) {
            cv_.wait(lock);
        }
        finalized = true;

        for(auto conn: connections_)
        {
            conn.disconnect();
        }

        connections_.clear();
        ROS_DEBUG("[CallbackCounterSemaphore] callbacks finalized %s %ld",name_.c_str(), (long)this);
    }

    void addConnection(boost::signals2::connection conn) 
    {
        std::unique_lock<std::mutex> lock(mutex_);

        if(finalized)
        {
            ROS_DEBUG("[CallbackCounterSemaphore] ignoring adding callback, already finalized %s %ld",name_.c_str(), (long)this);
            return;
        }

        connections_.push_back(conn);
    }

private:
    int count_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<boost::signals2::connection> connections_;
    bool finalized = false;
    std::string name_;
};
}
