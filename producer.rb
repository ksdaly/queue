require 'nsq'

class Producer
  attr_accessor :worker, :queue

  def initialize(messages=[])
    @queue = messages
    @worker = Nsq::Producer.new(
      nsqlookupd: ['127.0.0.1:4361', '127.0.0.1:4363'],
      topic: 'videos'
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
    @threads ||= [get_message_thread]
  end

  def get_message_thread
    Thread.new do
      execute_messages
    end
  end

  def execute_messages
    queue.each do |message|
      worker.write_to_topic('videos', message)
    end
  end
end
