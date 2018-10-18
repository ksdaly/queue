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
    start
  end

  def start
    batch_message_thread = get_batch_message_thread
    message_thread = get_message_thread

    batch_message_thread.join
    message_thread.join
  rescue Exception
    stop
  end

  def stop
    worker.terminate
  end

  def get_message_thread
    Thread.new do
      loop do
        message = worker.pop
        processor.process(Message.new(JSON.parse(message.body, symbolize_names: true)))
        message.finish
      end
    end
  end

  def get_batch_message_thread
    Thread.new do
      loop do
        start = Time.now
        processor.batch_process
        sleep(start.to_i + TIK - Time.now.to_i)
      end
    end
  end
end
