<?php
/**
 * This file contains a PHP client to Celery distributed task queue
 *
 * LICENSE: 2-clause BSD
 *
 * Copyright (c) 2014, GDR!
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * The views and conclusions contained in the software and documentation are those
 * of the authors and should not be interpreted as representing official policies,
 * either expressed or implied, of the FreeBSD Project.
 *
 * @link http://massivescale.net/
 * @link http://gdr.geekhood.net/
 * @link https://github.com/gjedeer/celery-php
 *
 * @package celery-php
 * @license http://opensource.org/licenses/bsd-license.php 2-clause BSD
 * @author GDR! <gdr@go2.pl>
 */

require_once('amqp.php');

use App\Utils\MqConfirm;
use Dtsf\Core\Log;
use Dtsf\Core\WorkerApp;
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * Driver for pure PHP implementation of AMQP protocol
 * @link https://github.com/videlalvaro/php-amqplib
 * @package celery-php
 */
class AMQPLibConnector extends AbstractAMQPConnector
{
    const workerStop = 3;
    /**
     * How long (in seconds) to wait for a message from queue
     * Sadly, this can't be set to zero to achieve complete asynchronity
     */
    public $wait_timeout = 0.1;
    
    private $connection = null;
    private $connectionDetails = [];
    private $channels = [];
    private $confirmTickStatus = true;   //true 可处理, false 处理中
    private $confirmTick = 0;
    private $confirmAckTickTime = 1000; //单位s
    private $wokerStopFlag = 1;
    private $waitChan = null;  //该chan为了等待连接池中链接达到最大空闲时间后gc的时候等待最后ack完成在返回true, 否则该链接对象会被直接unset
    private $confirmMode = 0;
    private $chanNum = 1;
    
    
    /**
     * PhpAmqpLib\Message\AMQPMessage object received from the queue
     */
    private $message = null;
    
    /**
     * AMQPChannel object cached for subsequent GetMessageBody() calls
     */
    private $receiving_channel = null;
    
    function GetConnectionObject($details)
    {
        $this->connectionDetails = $details;
        $this->connection = new AMQPConnection(
            $details['host'],
            $details['port'],
            $details['login'],
            $details['password'],
            $details['vhost'],
            false,
            'AMQPLAIN',
            null,
            'en_US',
            $details['socket_connect_timeout'],
            $details['socket_timeout'],
            $details['context'],
            $details['keepalive'],
            $details['heartbeat']
        );
        
        $this->waitChan = new \chan(1);
        $this->channels[$details['exchange']] = new \chan(2);
        $this->setChannel($details['exchange']);
        return $this->connection;
    }
    
    
    /**
     * 创建一个channel
     *
     * @param $exchange
     */
    private function setChannel($exchange)
    {
        if (!empty($this->connectionDetails['return_callback']) ||
            !empty($this->connectionDetails['confirm_ack_callback']) ||
            !empty($this->connectionDetails['confirm_nack_callback'])
        ) {
            $this->confirmMode = 1;
        }
        
        for ($i = 0; $i < $this->chanNum; $i++) {
            $channel = $this->connection->channel();
            $this->storeChan($channel, $exchange);
        }
        
        if ($this->confirmMode) {
            $this->listenEvent();
        }
    }
    
    /**
     * 保存chan
     * @param $channel
     * @param $exchange
     */
    private function storeChan($channel, $exchange)
    {
        if ($this->confirmMode) {
            $channel->set_nack_handler(
                function (AMQPMessage $message) {
                    if (is_callable($this->connectionDetails['confirm_nack_callback'])) {
                        call_user_func($this->connectionDetails['confirm_nack_callback'], $message);
                    }
                }
            );
            $channel->set_ack_handler(
                function (AMQPMessage $message) {
                    if (is_callable($this->connectionDetails['confirm_ack_callback'])) {
                        call_user_func($this->connectionDetails['confirm_ack_callback'], $message);
                    }
                }
            );
            $channel->set_return_listener(function ($replyCode, $replyText, $exchange, $routingKey, $message) {
                if (is_callable($this->connectionDetails['return_callback'])) {
                    call_user_func_array($this->connectionDetails['return_callback'], [
                        $replyCode,
                        $replyText,
                        $exchange,
                        $routingKey,
                        $message
                    ]);
                }
            });
            $channel->confirm_select();
        }
        $this->pushChan($channel, $exchange);
    }
    
    
    /**
     * 清空当前链接的所有chan
     */
    private function cleanChan()
    {
        $this->channels = [];
    }
    
    /**
     * channel 监听
     */
    private function listenEvent()
    {
        if ($this->confirmTick) {
            $this->cleanTimer();
        }
        $this->confirmTick = swoole_timer_tick($this->confirmAckTickTime, [$this, 'handlerConfirm']);
    }
    
    /**
     * clean timer
     */
    private function cleanTimer()
    {
        if ($this->confirmTick > 0) {
            swoole_timer_clear($this->confirmTick);
            $this->confirmTick = -1;
        }
    }
    
    /**
     * 处理监听程序,时间回调默认在协程中启动
     * 根据定时时间周期执行
     * 直到worker退出事件触发, 并标识workerStopFlag为3, 此时清除定时器,并断开链接
     */
    public function handlerConfirm()
    {
        try {
            if (!$this->connection->isConnected()) {
                $this->cleanTimer();
            }
            //如果还在处理中那么直接返回
            if (!$this->confirmTickStatus) {
                return;
            }
            $this->confirmTickStatus = false;
            $chan = $this->popChan($this->connectionDetails['exchange'], 0.01, $beforeUseTryTimes = 3);
            if (!empty($chan) && $chan->getConnection()->isConnected()) {
                $chan->wait_for_pending_acks_returns();
                //要执行下面if中的语句,说明gc执行后最后一次confirm已经执行完毕 line217
                if ($this->getWokerStopFlag() == self::workerStop) {
                    //不能断开该链接,因为协程可能还没执行完毕
                    \Swoole\Coroutine::sleep(2);
                    $this->ackFinish();
                }
            }
            $this->confirmTickStatus = true;
            $this->pushChan($chan);
        } catch (\Throwable $throwable) {
            $this->connection->close();
            $this->confirmTickStatus = true;
            Log::error("{worker_id} ack error on handlerConfirm, and current app status is {status}, msg: {msg}."
                , [
                    '{worker_id}' => posix_getpid(),
                    '{status}' => WorkerApp::getInstance()->serverStatus,
                    '{msg}' => $throwable->getMessage() . '====trace:' . $throwable->getTraceAsString()
                ]
                , WorkerApp::getInstance()->ackErrorDirName);
        }
    }
    
    /**
     * 获取一个chann
     *
     * @param $exchange
     * @param int $beforeUseTryTimes
     * @return mixed
     */
    private function popChan($exchange, $popTime = 0.1, $retryTimes = 500)
    {
        $channel = $this->channels[$exchange]->pop($popTime);
        if ($retryTimes <= 0) {
            if (empty($channel)) {
                $this->cleanTimer();
                $this->GetConnectionObject($this->connectionDetails);
            }
            
            return null;
        }
        if (!is_object($channel) && $this->connection->isConnected() && $this->confirmTick > 0) {
            Log::warning('pid:{worker_id} objhash is {obj} reget channel of mq:' . $popTime . '--retryTimes:' . $retryTimes,
                ['{worker_id}' => posix_getpid()], 'pop_channel');
            return $this->popChan($exchange, $popTime, $retryTimes--);
        }
        
        return $channel;
    }
    
    /**
     * 该链接是供confirm和publish message使用
     * @param $chan
     * @param $exchange
     * @param $type 类型 post | confirm
     */
    private function pushChan($chan, $exchange = null)
    {
        if (empty($exchange)) {
            $exchange = $this->connectionDetails['exchange'];
        }
        $this->channels[$exchange]->push($chan);
    }
    
    /**
     * @param null $exchange
     * @return mixed
     */
    public function getChanLength($exchange = null)
    {
        if (empty($exchange)) {
            $exchange = $this->connectionDetails['exchange'];
        }
        return $this->channels[$exchange]->length();
    }
    
    /**
     * gc 只会调用一次, worker退出会频繁调用,直到worker退出,
     */
    public function workerExitHandlerConfirm()
    {
        try {
            go(function () {
                $this->cleanTimer();
                $chan = $this->popChan($this->connectionDetails['exchange'], 0.01);
                //正常的gc机制,某个连接对象在执行gc的时候只会执行一次
                if (!empty($chan) && $this->getWokerStopFlag() !== self::workerStop) {
                    $chan->wait_for_pending_acks_returns();
                    $this->setWokerStopFlag(self::workerStop);
                    $this->pushChan($chan);
                }
            });
        } catch (\Throwable $throwable) {
            Log::error("{worker_id} ack error on workerExitHandlerConfirm, and current app status is {status}, msg: {msg}."
                , [
                    '{worker_id}' => posix_getpid(),
                    '{status}' => WorkerApp::getInstance()->serverStatus,
                    '{msg}' => $throwable->getMessage() . '====trace:' . $throwable->getTraceAsString()
                ]
                , WorkerApp::getInstance()->ackErrorDirName);
        }
    }
    
    /**
     * 等待最后一次timer执行wait_for_pending_acks_returns, 并退出
     * @return mixed
     */
    private function waitToStop()
    {
        return $this->waitChan->pop();
    }
    
    /**
     * 完成最后ack
     */
    private function ackFinish()
    {
        $this->cleanTimer();
        //不能断开该链接,因为协程可能还没执行完毕
        $this->connection->close();
        //设置worker状态为确认最后ack完成
        $this->waitChan->push(1);
        WorkerApp::getInstance()->setWorkerStatus(WorkerApp::WORKERLASTACK);
        Log::warning("worker {worker_id} exec last ack at tick, and current app status is {status}.",
            ['{worker_id}' => posix_getpid(), '{status}' => WorkerApp::getInstance()->serverStatus],
            WorkerApp::getInstance()->debugDirName);
    }
    
    /**
     * 获取worker状态
     * @return int
     */
    private function getWokerStopFlag()
    {
        return $this->wokerStopFlag;
    }
    
    /**
     * @param int $flag
     * 设置worker状态
     */
    private function setWokerStopFlag(int $flag)
    {
        $this->wokerStopFlag = $flag;
    }
    
    /* NO-OP: not required in PhpAmqpLib */
    function Connect($connection)
    {
    }
    
    /**
     * Return an AMQPTable from a given array
     * @param array $headers Associative array of headers to convert to a table
     */
    private function HeadersToTable($headers)
    {
        return new AMQPTable($headers);
    }
    
    /**
     * Post a task to exchange specified in $details
     * @param AMQPConnection $connection Connection object
     * @param array $details Array of connection details
     * @param string $task JSON-encoded task
     * @param array $params AMQP message parameters
     */
    function PostToExchange($connection, $details, $task, $params, $headers)
    {
        $chan = $this->popChan($this->connectionDetails['exchange'], 0.01, 100);
        $application_headers = $this->HeadersToTable($headers);
        $params['application_headers'] = $application_headers;
        try {
            $msg = new AMQPMessage(
                $task,
                $params
            );
            !empty($chan) && $chan->basic_publish($msg, $details['exchange'], $details['routing_key']);
        } catch (AMQPRuntimeException $e) {
            //清除老对象的定时器
            $this->cleanTimer();
            //运行时异常关闭链接
            $this->connection->close();
            throw $e;
        } catch (AMQPTimeoutException $e) {
            //清除老对象的定时器
            $this->cleanTimer();
            //从新生产对象, 这里如果是因为超时并收到rst标志位导致的链接已经关闭,
            // 这里在执行关闭会有问题, 因此采用重连并初始化chan的操作
            $this->GetConnectionObject($this->connectionDetails);
            throw $e;
        }
        $this->pushChan($chan, $this->connectionDetails['exchange']);
        
        return true;
    }
    
    /**
     * A callback function for AMQPChannel::basic_consume
     * @param PhpAmqpLib\Message\AMQPMessage $msg
     */
    function Consume($msg)
    {
        $this->message = $msg;
    }
    
    /**
     * Return result of task execution for $task_id
     * @param object $connection AMQPConnection object
     * @param string $task_id Celery task identifier
     * @param int $expire expire time result queue, milliseconds
     * @param boolean $removeMessageFromQueue whether to remove message from queue
     * @return array array('body' => JSON-encoded message body, 'complete_result' => AMQPMessage object)
     *            or false if result not ready yet
     */
    function GetMessageBody($connection, $task_id, $expire = 0, $removeMessageFromQueue = true)
    {
        if (!$this->receiving_channel) {
            $ch = $connection->channel();
            $expire_args = null;
            if (!empty($expire)) {
                $expire_args = array("x-expires" => array("I", $expire));
            }
            
            $ch->queue_declare(
                $task_id,        /* queue name */
                false,            /* passive */
                true,            /* durable */
                false,            /* exclusive */
                true,            /* auto_delete */
                false,                  /* no wait */
                $expire_args
            );
            
            $ch->basic_consume(
                $task_id,        /* queue */
                '',            /* consumer tag */
                false,            /* no_local */
                false,            /* no_ack */
                false,            /* exclusive */
                false,            /* nowait */
                array($this, 'Consume')    /* callback */
            );
            $this->receiving_channel = $ch;
        }
        
        try {
            $this->receiving_channel->wait(null, false, $this->wait_timeout);
        } catch (PhpAmqpLib\Exception\AMQPTimeoutException $e) {
            return false;
        }
        
        /* Check if the callback function saved something */
        if ($this->message) {
            if ($removeMessageFromQueue) {
                $this->receiving_channel->queue_delete($task_id);
            }
            $this->receiving_channel->close();
            $connection->close();
            
            return array(
                'complete_result' => $this->message,
                'body' => $this->message->body, // JSON message body
            );
        }
        
        return false;
    }
}

