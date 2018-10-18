require 'json'
require_relative './video'
require_relative './message'

class Processor
  REALTIME_LIMIT = 100
  PUBLSIHER_LIMIT = 20

  def buffer
    @buffer ||= Hash.new(0)
  end

  def reset_buffer
    @buffer = Hash.new(0)
  end

  def process(message)
    video = Video.find(message.video_id)
    count = video.get(:video_played)

    if count < REALTIME_LIMIT
      count = video.increment(:video_played, message.qty)
      publish(progress(video.id, count))
    else
      buffer[message.video_id] += message.qty
    end
  end

  def batch_process
    copy = buffer
    reset_buffer

    copy.keys.each_slice(PUBLSIHER_LIMIT) do |batch|
      cache = []
      batch.each do |video_id|
        message = Message.new(video_id: video_id, qty: copy[video_id])
        video = Video.find(message.video_id)
        count = video.increment(:video_played, message.qty)
        cache << progress(video.id, count)
      end
      publish(cache)
    end
  end

  def publish(data)
    case data
    when Hash
      puts data.to_json
    when Array
      puts data.inject(:merge).to_json
    end
  end

  def progress(id, count)
    {
      id => count
    }
  end
end
