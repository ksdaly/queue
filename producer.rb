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
    start
  end

  def start
    message_thread = get_message_thread
    message_thread.join
  end

  def stop
    worker.terminate
  rescue Exception
    stop
  end

  def get_message_thread
    Thread.new do
      queue.each do |message|
        worker.write_to_topic('videos', message)
      end
    end
  end
end
