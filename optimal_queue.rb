#!/usr/bin/env ruby

require 'simple_uuid'
require 'json'
require 'nsq'
require 'time'
require 'pry'


class Video
  attr_accessor :id, :stats

  @@videos = {}

  def initialize
    @id = SimpleUUID::UUID.new.to_guid
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

class Message < Struct.new(:video_id, :qty, :occurred_at)
  def to_json
    {
      video_id: video_id,
      qty: qty.to_i,
      occurred_at: occurred_at.iso8601
    }.to_json
  end
end

videos = {}
messages = []
batch_messages = []
time_range_end = Time.now.to_i
time_range_start = time_range_end - 300

100.times do |n|
  Video.new.tap do |video|
    video.save
    plays = n < 20 ? 40 : 1240

    plays.times do
      messages << Message.new(video.id, 1, Time.at(rand(time_range_start...time_range_end)))
    end
  end
end

messages.shuffle!

class Processor
  @@cache = Hash.new(0)

  def self.cache
    @@cache
  end

  def self.process(message)
    video = Video.find(message.video_id)
    count = video.get(:video_played)
    
    if count < 100
      count = video.increment(:video_played, message.qty)
      puts "play count for #{ message.video_id }: #{ count }"
    else
      @@cache[message.video_id] += message.qty
    end
  end

  def self.batch_process
    copy = @@cache
    @@cache = Hash.new(0)

    copy.to_a.each_slice(20) do |messages|
      counts = {}
      messages.each do |message|
        video = Video.find(message[0])
        count = video.increment(:video_played, message[1])
        counts[message[0]] = count
      end
      puts "play count for batch: #{ counts }"
    end
  end
end

# process = true
# TIK = 60

# while process
#   start = Time.now

#   # process batch messages
#   Processor.batch_process

#   sleep(start.to_i + TIK - Time.now.to_i)
# end

n = 0
messages.each do |message|
  Processor.process(message)
  n += 1
  Processor.batch_process if n % 10_000 == 0
end

Video.all.each do |id, video|
  puts "total count: #{ video.get(:video_played) }"
end
