/**
 * @file status_machine.hpp
 * @author chenpi (chenpi@duck.com)
 * @brief A common status machine for stream protocol
 * @version 0.2
 * @date 2023-06-16
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <unordered_map>
#include <list>
#include <atomic>
#include <string>
#include <tuple>
#include <memory>
#include <functional>
#include <future>
#include <thread>
#include <mutex>
#include <condition_variable>

class StreamInterface
{
public:
    StreamInterface() = default;
    virtual ~StreamInterface() = default;

    /**
     * @brief Open the interface
     *
     * @return int On success, 0 is returned
     * @attention Can be called multiple times
     */
    virtual int Open() = 0;

    /**
     * @brief Close the interface
     *
     * @attention Can be called even if the interface is not open
     */
    virtual void Close() = 0;

    /**
     * @brief Write data to interface
     *
     * @param data data to write
     * @param length data length
     * @param timeout timeout in ms
     * @return size_t The number of bytes written
     * @attention Never block
     */
    virtual size_t Write(uint8_t const *data, size_t const length, size_t const timeout) = 0;

    /**
     * @brief Read data from interface
     *
     * @param data data to read
     * @param length data length
     * @param timeout timeout in ms
     * @return size_t The number of bytes written
     * @attention Never block
     */
    virtual size_t Read(uint8_t *data, size_t const length, size_t const timeout) = 0;

    /**
     * @brief find a full package in given buffer
     *
     * @param data buffer data
     * @param length buffer length
     * @return <uint8_t const * first, uint8_t const * last, uint32_t package_type>
     * @return On Success, the returned package is [first, last), the returned package_type is not std::numeric_limits<uint32_t>::max().
     * @return On failure, the returned package_type is std::numeric_limits<uint32_t>::max()
     *
     * @attention the inner buffer's head will always be set to last if last is not nullptr
     */
    virtual std::tuple<uint8_t const *, uint8_t const *, uint32_t> FindPackage(uint8_t const *data, size_t const length) = 0;
};

template <size_t MaxBufferSize, size_t ReadTimeout = 1000, size_t WriteTimeout = 1000>
class StreamMachine
{
public:
    using CallbackFunction = std::function<void(uint8_t const *data, size_t const length)>;
    using Handler = std::tuple<uint32_t const, std::list<CallbackFunction>::const_iterator>;
    using PredictFunction = std::function<bool(uint8_t const *data, size_t const length)>;

public:
    StreamMachine(StreamMachine const &) = delete;
    StreamMachine(StreamMachine &&) = delete;
    StreamMachine &operator=(StreamMachine const &) = delete;
    StreamMachine &operator=(StreamMachine &&) = delete;

public:
    StreamMachine(std::shared_ptr<StreamInterface> interface) : interface_(interface)
    {
    }

    ~StreamMachine()
    {
        this->Off();
    }

    /**
     * @brief Turn on the machine, this will call StreamInterface::Open
     *
     * @return int On success, 0 is returned
     */
    int On()
    {
        auto ret = interface_->Open();
        if (ret == 0)
        {
            exit_flag_ = false;
            recv_thread_ = std::thread(&StreamMachine::Recv, this);
            handle_thread_ = std::thread(&StreamMachine::PackageHandler, this);
        }
        exit_flag_ = false;
        return ret;
    }

    /**
     * @brief Turn off the machine, this will call StreamInterface::Close
     *
     */
    void Off()
    {
        exit_flag_ = true;
        if (recv_thread_.joinable())
        {
            recv_thread_.join();
        }
        if (handle_thread_.joinable())
        {
            package_condition_.notify_all();
            handle_thread_.join();
        }
        interface_->Close();
        package_queue_.clear();
    }

    /**
     * @brief Register a callback function to specific package type
     *
     * @param type package type, register to all package when set to std::numeric_limits<uint32_t>::max()
     * @param f callback function
     * @return Handler The handler is needed in deregister
     */
    Handler Register(uint32_t const type, CallbackFunction &&f)
    {
        // emplace handler
        std::lock_guard<std::mutex> lock(register_map_mutex_);
        register_map_[type].emplace_back(std::move(f));

        return std::make_tuple(type, --register_map_[type].cend());
    }

    /**
     * @brief Deregister a callback function
     *
     * @param hdl registered handler
     * @attention The hdl shall not be used after deregister
     */
    void Deregister(Handler &hdl)
    {
        uint32_t type;
        std::list<CallbackFunction>::const_iterator it;

        // erase handler
        std::lock_guard<std::mutex> lock(register_map_mutex_);
        std::tie(type, it) = hdl;
        if (register_map_.count(type))
        {
            register_map_[type].erase(it);
            if (register_map_[type].empty())
            {
                register_map_.erase(type);
            }
        }
    }

    /**
     * @brief Send data to interface
     *
     * @param data data to send
     * @param length data length
     * @param timeout timeout in ms, block when set to 0
     * @return size_t The number of bytes sent
     */
    size_t Send(uint8_t const *data, size_t const length, size_t const timeout = WriteTimeout)
    {
        size_t sent_length = 0;
        if (timeout)
        {
            ssize_t time_left = timeout;
            auto time_start = std::chrono::system_clock::now();
            while (!exit_flag_ && time_left > 0)
            {
                sent_length += interface_->Write(data + sent_length, length - sent_length, time_left);
                time_left -= std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - time_start).count();
            }
        }
        else
        {
            while (!exit_flag_)
            {
                sent_length += interface_->Write(data + sent_length, length - sent_length, timeout);
            }
        }
        return sent_length;
    }

    /**
     * @brief Send data to interface and wait for an ack package
     *
     * @param data data to send
     * @param length data length
     * @param type ack package type, register to all package when set to 0
     * @param predict the function should return true if an ack package is accepted
     * @param once run predict only once (accept any registered package no matter what predict returns)
     * @param timeout timeout in ms, block when set to 0
     * @return int On success, 0 is returned. 1 is returned when once==true && predict()==false,
     */
    int SendWithAck(uint8_t const *data, size_t const length, uint32_t const type, PredictFunction const &predict, bool const once = true, size_t const timeout = WriteTimeout)
    {
        auto ack_promise = std::make_shared<std::promise<bool>>();
        auto handler = Register(type,
                                [ack_promise, once, &predict](uint8_t const *data, size_t const length)
                                {
                                    bool ret = predict(data, length);
                                    if (once || ret)
                                    {
                                        try
                                        {
                                            ack_promise->set_value(ret);
                                        }
                                        catch (...)
                                        {
                                        }
                                    }
                                });

        if (length != Send(data, length, timeout))
        {
            return -1;
        }

        auto ack_future = ack_promise->get_future();
        std::future_status ack_status = std::future_status::ready;
        if (timeout)
        {
            ack_status = ack_future.wait_for(std::chrono::milliseconds(timeout));
        }
        else
        {
            ack_future.wait();
        }
        Deregister(handler);

        if (ack_status != std::future_status::ready)
        {
            return -2;
        }

        return ack_future.get() ? 0 : 1;
    }

private:
    // flag
    std::atomic_bool exit_flag_;

    // interface
    std::shared_ptr<StreamInterface> interface_;

    // package control
    std::list<std::tuple<uint32_t, std::unique_ptr<uint8_t[]>, size_t>> package_queue_;
    std::mutex package_mutex_;
    std::condition_variable package_condition_;

    // register
    std::mutex register_map_mutex_;
    std::unordered_map<uint32_t, std::list<CallbackFunction>> register_map_;

    // background thread
    std::thread recv_thread_;
    std::thread handle_thread_;

private:
    void Recv()
    {
        std::unique_ptr<uint8_t[]> buffer(new uint8_t[MaxBufferSize]);
        size_t buffer_length = 0;

        uint8_t const *package_start = nullptr, *package_end = nullptr;
        uint32_t package_type = 0;

        while (!exit_flag_)
        {
            // read to buffer
            auto read_length = interface_->Read(&buffer[buffer_length], MaxBufferSize - buffer_length, ReadTimeout);
            if (read_length == 0)
            {
                continue;
            }
            buffer_length += read_length;

            // find package
            do
            {
                std::tie(package_start, package_end, package_type) = interface_->FindPackage(buffer.get(), buffer_length);

                // package queue push_back
                if (package_type != std::numeric_limits<uint32_t>::max())
                {
                    size_t package_length = package_end - package_start;
                    std::lock_guard<std::mutex> lock(package_mutex_);
                    package_queue_.emplace_back(std::make_tuple(
                        package_type,
                        std::unique_ptr<uint8_t[]>(new uint8_t[package_length]),
                        package_length));
                    std::memcpy(std::get<1>(package_queue_.back()).get(), package_start, package_length);
                    package_condition_.notify_all();
                }

                // memmove
                if (package_end != nullptr)
                {
                    buffer_length -= package_end - buffer.get();
                    if (package_end == buffer.get() || buffer_length == 0)
                    {
                        package_end = nullptr;
                    }
                    else
                    {
                        std::memmove(buffer.get(), package_end, buffer_length);
                    }
                }
            } while (package_end != nullptr);
        }
    }

    void PackageHandler()
    {
        while (!exit_flag_)
        {
            // pop from package queue
            uint32_t current_package_type = 0;
            size_t current_package_size = 0;
            std::unique_ptr<uint8_t[]> current_package;
            {
                std::unique_lock<std::mutex> lock(package_mutex_);
                package_condition_.wait(lock, [&]()
                                        { return exit_flag_.load() || !package_queue_.empty(); });
                if (exit_flag_)
                {
                    break;
                }
                current_package_type = std::get<0>(package_queue_.front());
                current_package_size = std::get<2>(package_queue_.front());
                current_package = std::move(std::get<1>(package_queue_.front()));
                package_queue_.pop_front();
            }

            // search and call functions
            std::lock_guard<std::mutex> lock(register_map_mutex_);
            // package_type
            {
                auto function_list_it = register_map_.find(current_package_type);
                if (function_list_it != register_map_.end())
                {
                    for (auto const &f : function_list_it->second)
                    {
                        f(current_package.get(), current_package_size);
                    }
                }
            }
            // all
            {
                auto fuction_list_all_it = register_map_.find(std::numeric_limits<uint32_t>::max());
                if (fuction_list_all_it != register_map_.end())
                {
                    for (auto const &f : fuction_list_all_it->second)
                    {
                        f(current_package.get(), current_package_size);
                    }
                }
            }
        }
    }
};
