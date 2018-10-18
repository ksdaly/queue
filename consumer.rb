require 'nsq'
require 'json'
require_relative './processor'
require_relative './message'

class Consumer
  attr_accessor :worker, :processor

  TIK = 60

  def initialize
    @processor = Processor.new
    @worker = Nsq::Consumer.new(
      nsqlookupd: ['127.0.0.1:4361', '127.0.0.1:4363'],
      topic: 'videos',
      channel: 'default'
    )
    sleep(3)
  end

  def start
    threads.each(&:join)
  rescue Exception
    stop
  end

  def stop
    threads.each(&:kill)
    worker.terminate
  end

  def threads
    @threads ||= [get_batch_message_thread, get_message_thread]
  end

  def get_message_thread
    Thread.new do
      loop do
        execute_message
      end
    end
  end

  def get_batch_message_thread
    Thread.new do
      loop do
        start = Time.now
        execute_batch_message
        sleep(start.to_i + TIK - Time.now.to_i)
      end
    end
  end

  def execute_message
    if message = worker.pop
      processor.process(Message.new(JSON.parse(message.body, symbolize_names: true)))
      message.finish
    end
  end

  def execute_batch_message
    processor.batch_process
  end
end
