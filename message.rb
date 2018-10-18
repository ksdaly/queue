require 'json'
require_relative './video'

class Message < OpenStruct
  def self.seed
     messages = []

    100.times do |n|
      Video.new.tap do |video|
        video.save
        plays = n < 20 ? 40 : 1240

        plays.times do
          messages << new(video_id: video.id, qty: 1).to_json
        end
      end
    end

    messages.shuffle
  end

  def to_json
    {
      'video_id': video_id,
      'qty': qty
    }.to_json
  end
end
