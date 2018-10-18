require 'securerandom'

class Video
  attr_accessor :id, :stats

  @@videos = {}

  def initialize
    @id = SecureRandom.uuid
    @stats = {
      video_played: 0
    }
  end

  def self.all
    @@videos
  end

  def self.find(id)
    @@videos[id]
  end

  def save
    @@videos[id] = self
  end

  def increment(key, val)
    stats[key] += val.to_i
  end

  def get(key)
    stats[key].to_i
  end
end
