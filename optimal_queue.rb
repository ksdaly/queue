#!/usr/bin/env ruby

require 'simple_uuid'
require 'json'
require 'nsq'
require 'nsq-cluster'
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

  def progress
    {
      count: get(:video_played)
    }
  end

  def increment(key, val)
    stats[key] += val.to_i
  end

  def get(key)
    stats[key].to_i
  end
end

class Message < OpenStruct
  def to_json
    {
      'video_id': video_id,
      'qty': qty.to_i,
      'occurred_at': occurred_at.iso8601
    }.to_json
  end
end

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
      puts "#{ video.id }: #{ video.progress }"
    else
      @@cache[message.video_id] += message.qty
    end
  end

  def self.batch_process
    copy = @@cache
    @@cache = Hash.new(0)

    copy.to_a.each_slice(20) do |batch|
      counts = {}
      batch.each do |data|
        message = Message.new(video_id: data[0], qty: data[1], occurred_at: Time.now)
        video = Video.find(message.video_id)
        count = video.increment(:video_played, message.qty)
        counts[message.video_id] = video.progress
      end
      puts "play count for batch: #{ counts }"
    end
  end
end

messages = []
time_range_end = Time.now.to_i
time_range_start = time_range_end - 300

100.times do |n|
  Video.new.tap do |video|
    video.save
    plays = n < 20 ? 40 : 1240

    plays.times do
      messages << Message.new(video_id: video.id, qty: 1, occurred_at: Time.at(rand(time_range_start...time_range_end))).to_json
    end
  end
end

cluster = NsqCluster.new(nsqd_count: 10, nsqlookupd_count: 1)
nsqd = cluster.nsqd.first
nsqd.create(topic: 'video_played')
nsqd.create(topic: 'video_played', channel: 'video')

producer = Nsq::Producer.new(
  topic: 'video_played'
)

consumer = Nsq::Consumer.new(
  nsqlookupd: cluster.nsqlookupd.map { |nsqlookupd|
    "#{nsqlookupd.host}:#{nsqlookupd.http_port}"
  },
  topic: 'video_played',
  channel: 'video'
)

consumer2 = Nsq::Consumer.new(
  nsqlookupd: cluster.nsqlookupd.map { |nsqlookupd|
    "#{nsqlookupd.host}:#{nsqlookupd.http_port}"
  },
  topic: 'batch',
  channel: 'video'
)

begin
  messages.shuffle!

  messages.each_slice(10_000) do |batch|
    nsqd.mpub('video_played', *batch)
  end  

  while true
    message = consumer.pop
    Processor.process(Message.new(JSON.parse(message.body, symbolize_names: true)))
    message.finish
  end
ensure
  cluster.destroy
  producer.terminate
  consumer.terminate
  consumer2.terminate
end  


# TIK = 60
# while
#   start = Time.now
#   Processor.batch_process
#   break unless  messages.size > 0 || Processor.cache.size > 0
#   sleep(start.to_i + TIK - Time.now.to_i)
# end

